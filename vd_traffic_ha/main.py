#!/usr/bin/env python3
"""
vd-traffic-ha — Vejdirektoratet trafik-AMQP til Home Assistant MQTT bro.

Lytter på Vejdirektoratets Dataudveksler AMQP-feed (dataset 415,
"Traffic Events and Road Works"), parser DATEX II 3.2 XML-beskederne,
og publicerer dem til MQTT med Home Assistant auto-discovery.

Én situation (hændelse) = én sensor-entitet i HA, uanset hvor mange
situationRecord-underelementer (Accident/RoadOrCarriagewayOrLaneManagement/
Conditions/osv.) DATEX II opdeler den i. Alle underrecords ligger som en
liste i sensorens attributter.

Aktive hændelser med kendte koordinater publiceres desuden som en
MQTT device_tracker, så de dukker op automatisk på Home Assistants
indbyggede kort-dashboard. Trackeren fjernes igen når hændelsen lukkes.

Denne version filtrerer IKKE på geografisk område — alle hændelser i
feedet bliver publiceret. Områdefiltrering kan tilføjes senere.

Kører som Home Assistant add-on: config kommer fra /data/options.json
(sat via Supervisorens UI, se config.yaml for skema).
Kan også køres standalone: python main.py sti-til-config.yaml
(se ../config.example.yaml for et eksempel).
"""

from __future__ import annotations

import html
import json
import logging
import os
import signal
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import paho.mqtt.client as mqtt
import yaml
from azure.identity import ClientSecretCredential
from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode
from azure.servicebus.exceptions import ServiceBusError

log = logging.getLogger("vd-traffic-ha")


# --------------------------------------------------------------------------- #
# Reverse geocoding (fallback for hændelser uden location_description)
#
# Bruger OpenStreetMap Nominatim, som er gratis og ikke kræver en API-nøgle.
# Nominatims brugsvilkår kræver: en identificerende User-Agent, og højst ét
# opslag i sekundet — begge dele overholdes her. Resultater caches i
# hukommelsen (afrundet til ~100m) så vi aldrig slår den samme lokation op
# to gange i én kørsel.
# --------------------------------------------------------------------------- #

_NOMINATIM_USER_AGENT = "vd-traffic-ha-addon/1.0 (Home Assistant integration)"
_NOMINATIM_MIN_INTERVAL = 1.1  # sekunder mellem opslag, jf. Nominatims brugsvilkår


class ReverseGeocoder:
    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self._cache: dict[str, Optional[str]] = {}
        self._last_request = 0.0
        self._lock = threading.Lock()

    @staticmethod
    def _cache_key(lat: float, lon: float) -> str:
        # ~3 decimaler svarer til ca. 100 meters præcision — rigeligt til
        # en fornuftig stedbeskrivelse, og holder cachen effektiv.
        return f"{lat:.3f},{lon:.3f}"

    def resolve(self, lat: float, lon: float) -> Optional[str]:
        if not self.enabled:
            return None
        key = self._cache_key(lat, lon)
        with self._lock:
            if key in self._cache:
                log.debug("Geocoding cache-hit for %s -> %r", key, self._cache[key])
                return self._cache[key]

            wait = _NOMINATIM_MIN_INTERVAL - (time.time() - self._last_request)
            if wait > 0:
                time.sleep(wait)

            result = self._fetch(lat, lon)
            self._last_request = time.time()
            self._cache[key] = result
            if result:
                log.info("Geocodede %s -> %r", key, result)
            else:
                log.info("Kunne ikke geocode %s (se DEBUG-log for detaljer)", key)
            return result

    @staticmethod
    def _fetch(lat: float, lon: float) -> Optional[str]:
        params = urllib.parse.urlencode({
            "format": "jsonv2",
            "lat": f"{lat:.6f}",
            "lon": f"{lon:.6f}",
            "zoom": "16",
            "accept-language": "da",
        })
        url = f"https://nominatim.openstreetmap.org/reverse?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": _NOMINATIM_USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            log.debug("Reverse geocoding fejlede for %.5f,%.5f: %s", lat, lon, exc)
            return None

        addr = data.get("address", {})
        road = addr.get("road")
        city = addr.get("city") or addr.get("town") or addr.get("village") or addr.get("municipality")
        if road and city:
            return f"{road}, {city}"
        if road:
            return road
        return data.get("display_name")


# --------------------------------------------------------------------------- #
# Config
#
# yaml.safe_load parser gyldig JSON lige så fint som YAML, så samme
# funktion bruges uanset om vi kører som add-on (options.json) eller
# standalone (config.yaml).
# --------------------------------------------------------------------------- #

def load_config(path: str) -> dict:
    if not os.path.exists(path):
        log.error(
            "Config-fil '%s' findes ikke. Som add-on leverer Supervisor "
            "denne automatisk — som standalone-script skal du kopiere "
            "config.example.yaml til config.yaml først.", path
        )
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# --------------------------------------------------------------------------- #
# DATEX II parsing
#
# DATEX II-navnerum varierer på tværs af versioner/leverandører, så vi
# matcher på lokalt tag-navn (uden namespace) i stedet for at kræve et
# bestemt namespace. Det gør parseren mere robust, men mindre præcis —
# hvis felter mangler eller ser forkerte ud, sæt logging.dump_raw_xml: true
# i opsætningen og kig på den fulde XML for at justere logikken herunder.
# --------------------------------------------------------------------------- #

def _local(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _find_all(elem: ET.Element, name: str) -> list[ET.Element]:
    """Find alle elementer med et givet lokalt navn under elem, uanset namespace."""
    return [e for e in elem.iter() if _local(e.tag) == name]


def _text(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None or elem.text is None:
        return None
    stripped = elem.text.strip()
    if not stripped:
        return None
    # Kildedata er observeret dobbelt HTML-escaped (fx "&amp;lt;14&amp;gt;"
    # i stedet for "<14>") — unescape for et læsbart resultat.
    return html.unescape(stripped)


def _first_text(elem: ET.Element, name: str) -> Optional[str]:
    found = _find_all(elem, name)
    return _text(found[0]) if found else None


def _xsi_type(elem: ET.Element) -> Optional[str]:
    for key, value in elem.attrib.items():
        if _local(key) == "type":
            return value.split(":", 1)[-1]
    return None


@dataclass
class TrafficEvent:
    situation_id: Optional[str]
    record_id: Optional[str]
    record_type: Optional[str]
    version: Optional[str]
    creation_time: Optional[str]
    version_time: Optional[str]
    start_time: Optional[str]
    end_time: Optional[str]
    severity: Optional[str]
    probability_of_occurrence: Optional[str]
    safety_related_message: Optional[str]
    cause_type: Optional[str]
    number_of_operational_lanes: Optional[str]
    residual_lane_width: Optional[str]
    delay_band: Optional[str]
    delay_time_value: Optional[str]
    visibility_distance_m: Optional[str]
    comment: Optional[str]
    location_description: Optional[str]
    # Koordinater gemmes som den oprindelige tekst fra XML'en (ikke som
    # float), så vi aldrig introducerer afrunding i forhold til kilden.
    # latitude_num/longitude_num er float-udgaver til brug i device_tracker
    # og fremtidig polygon-filtrering.
    latitude: Optional[str]
    longitude: Optional[str]
    latitude_num: Optional[float]
    longitude_num: Optional[float]
    # Type-specifikke felter (fx accidentType, vehicleObstructionType,
    # roadMaintenanceType) — navngivningen varierer per hændelsestype
    # ifølge TRACÉ-protokolbeskrivelsen, så vi indsamler dem generisk i
    # stedet for at hardkode hvert feltnavn.
    extra_fields: dict[str, str] = field(default_factory=dict)


    @property
    def is_active(self) -> bool:
        if not self.end_time:
            return True
        try:
            end = datetime.fromisoformat(self.end_time.replace("Z", "+00:00"))
        except ValueError:
            return True
        return end > datetime.now(timezone.utc)

    @property
    def record_uid(self) -> str:
        rid = self.record_id or self.situation_id or "ukendt"
        return "".join(c if c.isalnum() else "_" for c in rid)

    @property
    def situation_uid(self) -> str:
        sid = self.situation_id or self.record_id or "ukendt"
        return "".join(c if c.isalnum() else "_" for c in sid)


# Direkte børn af situationRecord der allerede håndteres eksplicit andre
# steder — resten af de simple (blad-)børn er type-specifikke felter
# (fx accidentType, vehicleObstructionType, roadMaintenanceType, alive,
# temporarySpeedLimit) som vi indsamler generisk i stedet for at
# hardkode hvert feltnavn pr. hændelsestype.
_KNOWN_RECORD_CHILD_TAGS = {
    "situationRecordCreationTime",
    "situationRecordVersionTime",
    "probabilityOfOccurrence",
    "severity",
    "safetyRelatedMessage",
    "source",
    "validity",
    "impact",
    "cause",
    "generalPublicComment",
    "locationReference",
}


def _extract_extra_fields(record: ET.Element) -> dict[str, str]:
    """Indsamler simple (tekst-)værdier fra direkte børn der ikke allerede
    håndteres andre steder — typisk det type-specifikke felt som
    TRACÉ tilføjer pr. hændelsestype (fx sit:accidentType)."""
    extra: dict[str, str] = {}
    for child in list(record):
        local = _local(child.tag)
        if local in _KNOWN_RECORD_CHILD_TAGS:
            continue
        text = _text(child)
        if text is not None:
            extra[local] = text
        elif len(child) == 0:
            continue
        else:
            # Simpelt sammensat felt (fx visibility/delays under et andet
            # navn) — spring over her; sensorens 'records'-attribut har
            # stadig den fulde record_type til at slå detaljer op i loggen.
            continue
    return extra


def _extract_meta_fields(record: ET.Element) -> dict[str, Optional[str]]:
    """Udtrækker felter der er indlejrede (ikke simple bladelementer), og
    derfor ikke fanges af _extract_extra_fields: probabilityOfOccurrence,
    safetyRelatedMessage, cause/causeType, impact (antal spor, forsinkelse)
    og visibility (sigtbarhed for PoorEnvironmentConditions)."""
    result: dict[str, Optional[str]] = {
        "probability_of_occurrence": _first_text(record, "probabilityOfOccurrence"),
        "safety_related_message": _first_text(record, "safetyRelatedMessage"),
        "cause_type": _first_text(record, "causeType"),
        "number_of_operational_lanes": _first_text(record, "numberOfOperationalLanes"),
        "residual_lane_width": _first_text(record, "residualLaneWidth"),
        "delay_band": _first_text(record, "delayBand"),
        "delay_time_value": _first_text(record, "delayTimeValue"),
        "visibility_distance_m": _first_text(record, "integerMetreDistance"),
    }
    return result


def _extract_location(record: ET.Element) -> tuple[Optional[str], Optional[str], Optional[str], Optional[float], Optional[float]]:
    """Udtrækker (beskrivelse, lat_str, lon_str, lat_float, lon_float) fra en situationRecord."""
    location_description = None
    for desc_elem in _find_all(record, "locationDescription"):
        for value_elem in _find_all(desc_elem, "value"):
            text = _text(value_elem)
            if text:
                location_description = text
                break
        if location_description:
            break

    # Foretræk 'coordinatesForDisplay' (det centrale visningspunkt for
    # hændelsen) frem for punkter inde i en gmlLineString/pointByCoordinates,
    # som kan være mange og repræsenterer en vejstrækning snarere end ét sted.
    lat_str = lon_str = None
    coords_elem = _find_all(record, "coordinatesForDisplay")
    if coords_elem:
        lat_str = _first_text(coords_elem[0], "latitude")
        lon_str = _first_text(coords_elem[0], "longitude")
    if lat_str is None or lon_str is None:
        lat_elems = _find_all(record, "latitude")
        lon_elems = _find_all(record, "longitude")
        if lat_elems and lon_elems:
            lat_str = _text(lat_elems[0])
            lon_str = _text(lon_elems[0])
    if lat_str is None or lon_str is None:
        # LinearLocation-hændelser (fx en vejstrækning) angiver ofte kun
        # koordinater som en 'posList' — en enkelt tekststreng med
        # skiftevis lat/lon adskilt af mellemrum, uden separate
        # latitude/longitude-elementer. Brug første koordinatpar som
        # repræsentativt punkt.
        for pos_list_elem in _find_all(record, "posList"):
            text = _text(pos_list_elem)
            if not text:
                continue
            parts = text.split()
            if len(parts) >= 2:
                lat_str, lon_str = parts[0], parts[1]
                break

    lat_num = lon_num = None
    try:
        if lat_str is not None and lon_str is not None:
            lat_num = float(lat_str)
            lon_num = float(lon_str)
    except ValueError:
        pass

    return location_description, lat_str, lon_str, lat_num, lon_num


def parse_close_notifications(root: ET.Element) -> list[str]:
    """Parser en 'informationManagement'-luk-notifikation.

    Vejdirektoratet sender ind imellem en kort besked uden en fuld
    situation/situationRecord — kun en reference til at en tidligere
    udsendt situation nu er lukket. Returnerer situation-id'erne der
    er markeret som lukkede i denne besked (tom liste hvis ingen).
    """
    closed_situation_ids: list[str] = []
    for element_ref in _find_all(root, "elementReference"):
        status = _first_text(element_ref, "managementStatus")
        if status != "closed":
            continue
        for ref in _find_all(element_ref, "reference"):
            situation_id = ref.get("id")
            if situation_id:
                closed_situation_ids.append(situation_id)
    return closed_situation_ids


def parse_datex_root(body: bytes) -> Optional[ET.Element]:
    """Parser XML-bytes til et ElementTree-rodelement, eller None ved fejl."""
    try:
        return ET.fromstring(body)
    except ET.ParseError as exc:
        log.warning("Kunne ikke parse besked som XML: %s", exc)
        return None


def parse_datex_message(root: ET.Element) -> list[TrafficEvent]:
    """Udtrækker hændelser fra et allerede parset DATEX II-rodelement.

    En besked kan indeholde flere 'situation'-elementer, som hver kan
    indeholde flere 'situationRecord'-elementer (fx flere vejbaner
    berørt af samme hændelse).
    """
    events: list[TrafficEvent] = []
    situations = _find_all(root, "situation")
    if not situations:
        # Nogle beskeder kan være en enkelt situationRecord uden en
        # omsluttende 'situation' — prøv at parse hele roden som fallback
        situations = [root]

    for situation in situations:
        situation_id = situation.get("id")
        records = _find_all(situation, "situationRecord")
        for record in records:
            comment_parts = []
            for comment_elem in _find_all(record, "generalPublicComment"):
                for value_elem in _find_all(comment_elem, "value"):
                    text = _text(value_elem)
                    if text:
                        comment_parts.append(text)

            location_description, lat_str, lon_str, lat_num, lon_num = _extract_location(record)
            extra_fields = _extract_extra_fields(record)
            meta = _extract_meta_fields(record)

            events.append(
                TrafficEvent(
                    situation_id=situation_id,
                    record_id=record.get("id"),
                    record_type=_xsi_type(record),
                    version=record.get("version"),
                    creation_time=_first_text(record, "situationRecordCreationTime"),
                    version_time=_first_text(record, "situationRecordVersionTime"),
                    start_time=_first_text(record, "overallStartTime"),
                    end_time=_first_text(record, "overallEndTime"),
                    severity=_first_text(record, "severity"),
                    probability_of_occurrence=meta["probability_of_occurrence"],
                    safety_related_message=meta["safety_related_message"],
                    cause_type=meta["cause_type"],
                    number_of_operational_lanes=meta["number_of_operational_lanes"],
                    residual_lane_width=meta["residual_lane_width"],
                    delay_band=meta["delay_band"],
                    delay_time_value=meta["delay_time_value"],
                    visibility_distance_m=meta["visibility_distance_m"],
                    comment="; ".join(comment_parts) if comment_parts else None,
                    location_description=location_description,
                    latitude=lat_str,
                    longitude=lon_str,
                    latitude_num=lat_num,
                    longitude_num=lon_num,
                    extra_fields=extra_fields,
                )
            )

    return events


# --------------------------------------------------------------------------- #
# MQTT / Home Assistant discovery
#
# Én situation = én "sensor"-entitet, med alle dens situationRecords som
# en liste i attributterne. Hvis situationen har koordinater og er aktiv,
# holdes desuden en "device_tracker"-entitet ved lige, så den vises på
# Home Assistants indbyggede kort uden manuel dashboard-konfiguration.
# --------------------------------------------------------------------------- #

class HaMqttPublisher:
    """Publicerer VD-trafikhændelser til MQTT.

    I stedet for én sensor-entitet pr. hændelse (som vokser ubegrænset
    over tid, da lukkede hændelser aldrig fjernes), publiceres alle
    hændelser samlet i ÉN sensors attributter. Sensorens state er
    antallet af aktive hændelser.

    Aktive hændelser med kendte koordinater holdes desuden som
    individuelle MQTT device_tracker-entiteter, så de kan vises på
    Home Assistants indbyggede kort — de er selv-oprensende (fjernes
    helt fra HA når hændelsen lukkes), så de bidrager ikke til samme
    vækst-problem som de gamle per-hændelse-sensorer gjorde.
    """

    def __init__(self, cfg: dict, geocoding_enabled: bool = True):
        self.cfg = cfg
        self.base_topic = cfg["base_topic"].rstrip("/")
        self.discovery_prefix = cfg["discovery_prefix"].rstrip("/")
        self.client = mqtt.Client(client_id=cfg.get("client_id", "vd-traffic-bridge"))
        if cfg.get("username"):
            self.client.username_pw_set(cfg.get("username"), cfg.get("password") or None)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

        # situation_id -> { record_uid: TrafficEvent }
        self._situations: dict[str, dict[str, TrafficEvent]] = {}
        self._known_tracker_ids: set[str] = set()
        self._summary_discovered = False
        self.geocoder = ReverseGeocoder(enabled=geocoding_enabled)

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info("Forbundet til MQTT-broker")
        else:
            log.error("MQTT-forbindelse fejlede med kode %s", rc)

    def _on_disconnect(self, client, userdata, rc):
        log.warning("MQTT-forbindelse afbrudt (kode %s) — paho forsøger automatisk igen", rc)

    def connect(self):
        self.client.connect(self.cfg["host"], int(self.cfg.get("port", 1883)), keepalive=60)
        self.client.loop_start()

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

    def purge_stale_entities(self, wait_seconds: float = 3.0):
        """Rydder retained MQTT-discovery-beskeder fra tidligere kørsler.

        Broen holder kun styr på kendte sensorer/trackere i hukommelsen,
        så efter en genstart (eller en helt ny Home Assistant-installation
        mod samme broker) aner den ikke hvilke gamle vd_traffic-entiteter
        der stadig ligger som retained beskeder på broker'en — de bliver
        derfor aldrig fjernet af sig selv. Denne funktion lytter kort
        efter dem ved opstart og afregistrerer dem, så vi starter med en
        ren tavle hver gang. Normal drift genopretter herefter kun de
        sensorer/trackere der rent faktisk er aktive lige nu.
        """
        found_topics: set[str] = set()

        def _on_message(client, userdata, msg):
            if not msg.payload:
                return  # tomme (allerede-slettede) beskeder ignoreres
            # MQTT-wildcards kan kun matche et helt topic-niveau (+ må ikke
            # bruges som prefix inde i et niveau, fx 'vd_traffic_+' er
            # ugyldigt) — vi abonnerer derfor bredt på alle sensor/
            # device_tracker discovery-topics og filtrerer selv på
            # object_id-niveau i Python.
            parts = msg.topic.split("/")
            object_id = parts[-2] if len(parts) >= 2 else ""
            if object_id.startswith("vd_traffic"):
                found_topics.add(msg.topic)

        previous_handler = self.client.on_message
        self.client.on_message = _on_message

        wildcard_topics = [
            f"{self.discovery_prefix}/sensor/+/config",
            f"{self.discovery_prefix}/device_tracker/+/config",
        ]
        for topic in wildcard_topics:
            self.client.subscribe(topic)

        time.sleep(wait_seconds)

        for topic in wildcard_topics:
            self.client.unsubscribe(topic)
        self.client.on_message = previous_handler

        for topic in found_topics:
            self.client.publish(topic, "", retain=True)

        if found_topics:
            log.info(
                "Ryddede %d gamle vd_traffic-entitet(er) fra tidligere kørsler",
                len(found_topics),
            )
        else:
            log.info("Ingen gamle vd_traffic-entiteter fundet ved opstart")

    # -- samlet sensor (én for alle hændelser) --------------------------- #

    def _ensure_summary_discovery(self):
        if self._summary_discovered:
            return
        self._summary_discovered = True

        object_id = "vd_traffic_summary"
        config_topic = f"{self.discovery_prefix}/sensor/{object_id}/config"
        state_topic = f"{self.base_topic}/summary/state"
        attr_topic = f"{self.base_topic}/summary/attributes"

        payload = {
            "name": "VD Trafikhændelser",
            "unique_id": object_id,
            "state_topic": state_topic,
            "json_attributes_topic": attr_topic,
            "unit_of_measurement": "hændelser",
            "icon": "mdi:alert-octagon-outline",
            "device": {
                "identifiers": ["vd_traffic_bridge"],
                "name": "Vejdirektoratet Trafikhændelser",
                "manufacturer": "Vejdirektoratet",
                "model": "Dataudveksler AMQP",
            },
        }
        self.client.publish(config_topic, json.dumps(payload), retain=True)

    def _publish_summary(self):
        self._ensure_summary_discovery()
        state_topic = f"{self.base_topic}/summary/state"
        attr_topic = f"{self.base_topic}/summary/attributes"

        active_events = []
        for situation_uid, records in self._situations.items():
            if not any(r.is_active for r in records.values()):
                continue
            active_events.append(self._situation_summary_dict(situation_uid, records))

        attributes = {
            "active_events": active_events,
            "updated": datetime.now(timezone.utc).isoformat(),
        }

        self.client.publish(state_topic, str(len(active_events)), retain=True)
        self.client.publish(attr_topic, json.dumps(attributes), retain=True)
        log.info("Publicerede sammendrag: %d aktive hændelse(r)", len(active_events))

    def _situation_summary_dict(self, situation_uid: str, records: dict[str, TrafficEvent]) -> dict:
        named_record = next(
            (r for r in sorted(records.values(), key=lambda r: r.version_time or "", reverse=True)
             if r.location_description),
            next(iter(records.values())),
        )
        coord_record = next((r for r in records.values() if r.latitude and r.longitude), None)

        location_description = named_record.location_description
        geocoded = False
        if not location_description and coord_record and coord_record.latitude_num is not None:
            location_description = self.geocoder.resolve(
                coord_record.latitude_num, coord_record.longitude_num
            )
            geocoded = location_description is not None

        record_list = [
            {
                "record_id": r.record_id,
                "record_type": r.record_type,
                "version": r.version,
                "creation_time": r.creation_time,
                "version_time": r.version_time,
                "start_time": r.start_time,
                "end_time": r.end_time,
                "severity": r.severity,
                "probability_of_occurrence": r.probability_of_occurrence,
                "safety_related_message": r.safety_related_message,
                "cause_type": r.cause_type,
                "number_of_operational_lanes": r.number_of_operational_lanes,
                "residual_lane_width": r.residual_lane_width,
                "delay_band": r.delay_band,
                "delay_time_value": r.delay_time_value,
                "visibility_distance_m": r.visibility_distance_m,
                "comment": r.comment,
                "location_description": r.location_description,
                # Fuld original præcision som streng, så HA's frontend
                # ikke afrunder koordinaterne ved visning.
                "latitude": r.latitude,
                "longitude": r.longitude,
                "is_active": r.is_active,
                # Type-specifikke felter, fx accidentType, roadMaintenanceType.
                **r.extra_fields,
            }
            for r in records.values()
        ]
        return {
            "situation_id": next(iter(records.values())).situation_id,
            "location_description": location_description,
            "location_description_geocoded": geocoded,
            "latitude": coord_record.latitude if coord_record else None,
            "longitude": coord_record.longitude if coord_record else None,
            "records": record_list,
        }

    # -- device_tracker (kun for aktive hændelser med koordinater) ----- #

    def _ensure_tracker_discovery(self, situation_uid: str, name: str):
        if situation_uid in self._known_tracker_ids:
            return
        self._known_tracker_ids.add(situation_uid)

        object_id = f"vd_traffic_loc_{situation_uid}"
        config_topic = f"{self.discovery_prefix}/device_tracker/{object_id}/config"
        state_topic = f"{self.base_topic}/{situation_uid}/tracker_state"
        attr_topic = f"{self.base_topic}/{situation_uid}/tracker_attributes"

        payload = {
            "name": name,
            "unique_id": object_id,
            "state_topic": state_topic,
            "json_attributes_topic": attr_topic,
            "source_type": "gps",
            "icon": "mdi:map-marker-alert",
            "device": {
                "identifiers": ["vd_traffic_bridge"],
                "name": "Vejdirektoratet Trafikhændelser",
                "manufacturer": "Vejdirektoratet",
                "model": "Dataudveksler AMQP",
            },
        }
        self.client.publish(config_topic, json.dumps(payload), retain=True)

    def _remove_tracker(self, situation_uid: str):
        if situation_uid not in self._known_tracker_ids:
            return
        self._known_tracker_ids.discard(situation_uid)
        object_id = f"vd_traffic_loc_{situation_uid}"
        config_topic = f"{self.discovery_prefix}/device_tracker/{object_id}/config"
        # Tomt payload på discovery-config-topic afregistrerer entiteten i HA.
        self.client.publish(config_topic, "", retain=True)

    # -- offentlig API --------------------------------------------------- #

    def publish_event(self, event: TrafficEvent):
        situation_uid = event.situation_uid
        self._situations.setdefault(situation_uid, {})[event.record_uid] = event
        self._sync_tracker(situation_uid)
        self._publish_summary()

    def resync_all(self):
        """Genberegner status for ALLE kendte situationer, ikke kun den der
        senest fik en ny besked.

        'is_active' afhænger af nutidens klokkeslæt vs. overallEndTime —
        en hændelse kan altså gå fra aktiv til udløbet uden at der
        nogensinde kommer en ny besked om netop den. Sammendrags-sensoren
        beregnes allerede frisk hver gang, men kort-trackere opdateres kun
        når deres situation selv får en ny besked — uden dette periodiske
        kald ville udløbne trackere blive hængende for evigt. Kaldes fra
        en baggrundstråd med jævne mellemrum, se run().
        """
        for situation_uid in list(self._situations.keys()):
            self._sync_tracker(situation_uid)
        self._publish_summary()
        log.debug("Periodisk resync gennemført (%d kendte situationer)", len(self._situations))

    def close_situation(self, situation_id: str):
        """Marker en hel situation som lukket ud fra en luk-notifikation.

        Bruges når Vejdirektoratet sender en 'informationManagement'
        luk-notifikation uden fuld situationRecord-data. Hvis vi ikke har
        set situationen før (fx efter en genstart af broen), er der ikke
        noget at gøre.
        """
        situation_uid = "".join(c if c.isalnum() else "_" for c in situation_id)
        records = self._situations.get(situation_uid)
        if not records:
            log.debug(
                "Luk-notifikation for ukendt situation %s (ingen data at opdatere)",
                situation_id,
            )
            return
        for record in records.values():
            record.end_time = record.end_time or datetime.now(timezone.utc).isoformat()
        self._sync_tracker(situation_uid)
        self._publish_summary()
        log.info("Lukkede situation %s (%d record(er))", situation_id, len(records))

    def _sync_tracker(self, situation_uid: str):
        """Opretter/fjerner kort-markøren for en situation ud fra dens aktuelle status."""
        records = self._situations.get(situation_uid)
        if not records:
            return
        any_active = any(r.is_active for r in records.values())
        coord_record = next((r for r in records.values() if r.latitude_num is not None), None)

        if any_active and coord_record:
            summary = self._situation_summary_dict(situation_uid, records)
            short_id = situation_uid[:8]
            name = f"VD {summary['location_description'] or 'Trafikhændelse'} ({short_id})"
            self._ensure_tracker_discovery(situation_uid, name)
            tracker_state_topic = f"{self.base_topic}/{situation_uid}/tracker_state"
            tracker_attr_topic = f"{self.base_topic}/{situation_uid}/tracker_attributes"
            self.client.publish(tracker_state_topic, "hændelse", retain=True)
            self.client.publish(
                tracker_attr_topic,
                json.dumps({
                    "latitude": coord_record.latitude_num,
                    "longitude": coord_record.longitude_num,
                    "gps_accuracy": 50,
                }),
                retain=True,
            )
        else:
            self._remove_tracker(situation_uid)

# --------------------------------------------------------------------------- #
# AMQP receive loop
# --------------------------------------------------------------------------- #

_shutdown = False


def _handle_sigterm(signum, frame):
    global _shutdown
    log.info("Modtog stop-signal, lukker ned...")
    _shutdown = True


def run(config_path: str):
    cfg = load_config(config_path)

    log_level = str(cfg.get("logging", {}).get("level", "info")).upper()
    # Roden sættes til WARNING så tredjeparts-biblioteker (Azure SDK'et er
    # særligt pratsomt — dets 'cbs'-statustjek logger flere gange i
    # sekundet på DEBUG-niveau) ikke drukner vores egne beskeder. Kun
    # vd-traffic-ha-loggeren selv følger den konfigurerede level.
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log.setLevel(getattr(logging, log_level, logging.INFO))
    dump_raw_xml = bool(cfg.get("logging", {}).get("dump_raw_xml", False))

    az = cfg["azure"]
    credential = ClientSecretCredential(
        tenant_id=az["tenant_id"],
        client_id=az["client_id"],
        client_secret=az["client_secret"],
    )

    geocoding_enabled = bool(cfg.get("geocoding", {}).get("enabled", True))
    publisher = HaMqttPublisher(cfg["mqtt"], geocoding_enabled=geocoding_enabled)
    publisher.connect()
    publisher.purge_stale_entities()

    def _periodic_resync(interval_seconds: float = 120.0):
        while not _shutdown:
            time.sleep(interval_seconds)
            if _shutdown:
                break
            try:
                publisher.resync_all()
            except Exception:
                log.exception("Fejl under periodisk resync")

    threading.Thread(target=_periodic_resync, daemon=True).start()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    backoff = 5
    while not _shutdown:
        try:
            with ServiceBusClient(
                fully_qualified_namespace=az["fully_qualified_namespace"],
                credential=credential,
            ) as sb_client:
                with sb_client.get_subscription_receiver(
                    topic_name=az["topic_name"],
                    subscription_name=az["subscription_name"],
                    receive_mode=ServiceBusReceiveMode.PEEK_LOCK,
                    max_wait_time=30,
                ) as receiver:
                    log.info("Forbundet til AMQP-feed, venter på beskeder...")
                    backoff = 5  # reset efter succesfuld forbindelse
                    # 'for msg in receiver' afslutter sig selv efter
                    # max_wait_time sekunder uden nye beskeder — uden dette
                    # ydre while-loop ville det udløse en fuld reconnect
                    # (ny forbindelse + re-autentificering) hver gang feedet
                    # er stille i 30 sekunder. Her genoptager vi i stedet
                    # blot iterationen på den samme, stadig-åbne forbindelse.
                    while not _shutdown:
                        for msg in receiver:
                            if _shutdown:
                                break
                            try:
                                body = b"".join(msg.body) if hasattr(msg, "body") else bytes(msg)
                                if dump_raw_xml:
                                    log.debug("Rå DATEX II XML:\n%s", body.decode("utf-8", "replace"))

                                root = parse_datex_root(body)
                                if root is None:
                                    receiver.complete_message(msg)
                                    continue

                                events = parse_datex_message(root)
                                closed_situation_ids = parse_close_notifications(root)

                                for event in events:
                                    publisher.publish_event(event)
                                for situation_id in closed_situation_ids:
                                    publisher.close_situation(situation_id)

                                if not events and not closed_situation_ids:
                                    log.warning(
                                        "Ukendt beskedformat — hverken hændelser "
                                        "eller luk-notifikation fundet. Sæt "
                                        "dump_raw_xml: true i opsætningen for at "
                                        "undersøge strukturen."
                                    )
                                receiver.complete_message(msg)
                            except Exception:
                                log.exception("Fejl under behandling af besked — springer over")
                                try:
                                    receiver.abandon_message(msg)
                                except Exception:
                                    pass
        except ServiceBusError:
            log.exception("AMQP-forbindelsesfejl — prøver igen om %ss", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)
        except Exception:
            log.exception("Uventet fejl i modtage-loop — prøver igen om %ss", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)

    publisher.disconnect()
    log.info("Lukket ned.")


if __name__ == "__main__":
    config_file = sys.argv[1] if len(sys.argv) > 1 else "/data/options.json"
    run(config_file)

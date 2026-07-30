#!/usr/bin/env python3
"""
vd-traffic-ha — Vejdirektoratet trafik-AMQP til Home Assistant MQTT bro.

Lytter på Vejdirektoratets Dataudveksler AMQP-feed (dataset 415,
"Traffic Events and Road Works"), parser DATEX II 3.2 XML-beskederne,
og publicerer dem til MQTT med Home Assistant auto-discovery.

Denne version filtrerer IKKE på geografisk område — alle hændelser i
feedet bliver publiceret. Områdefiltrering kan tilføjes senere.

Kører som Home Assistant add-on: config kommer fra /data/options.json
(sat via Supervisorens UI, se config.yaml for skema).
Kan også køres standalone: python main.py sti-til-config.yaml
(se ../config.example.yaml for et eksempel).
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
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
    return elem.text.strip() or None


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
    comment: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    raw_extra: dict[str, Any] = field(default_factory=dict)

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
    def unique_id(self) -> str:
        rid = self.record_id or self.situation_id or "ukendt"
        return "".join(c if c.isalnum() else "_" for c in rid)


def parse_datex_message(body: bytes) -> list[TrafficEvent]:
    """Parser en DATEX II XML-besked og returnerer en liste af hændelser.

    En besked kan indeholde flere 'situation'-elementer, som hver kan
    indeholde flere 'situationRecord'-elementer (fx flere vejbaner
    berørt af samme hændelse).
    """
    events: list[TrafficEvent] = []
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        log.warning("Kunne ikke parse besked som XML: %s", exc)
        return events

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

            lat = lon = None
            lat_elem = _find_all(record, "latitude")
            lon_elem = _find_all(record, "longitude")
            if lat_elem and lon_elem:
                try:
                    lat = float(_text(lat_elem[0]))
                    lon = float(_text(lon_elem[0]))
                except (TypeError, ValueError):
                    pass

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
                    comment="; ".join(comment_parts) if comment_parts else None,
                    latitude=lat,
                    longitude=lon,
                )
            )

    return events


# --------------------------------------------------------------------------- #
# MQTT / Home Assistant discovery
# --------------------------------------------------------------------------- #

class HaMqttPublisher:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.base_topic = cfg["base_topic"].rstrip("/")
        self.discovery_prefix = cfg["discovery_prefix"].rstrip("/")
        self.client = mqtt.Client(client_id=cfg.get("client_id", "vd-traffic-bridge"))
        if cfg.get("username"):
            self.client.username_pw_set(cfg.get("username"), cfg.get("password") or None)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self._known_ids: set[str] = set()

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

    def _ensure_discovery(self, event: TrafficEvent):
        uid = event.unique_id
        if uid in self._known_ids:
            return
        self._known_ids.add(uid)

        object_id = f"vd_traffic_{uid}"
        config_topic = f"{self.discovery_prefix}/sensor/{object_id}/config"
        state_topic = f"{self.base_topic}/{uid}/state"
        attr_topic = f"{self.base_topic}/{uid}/attributes"

        payload = {
            "name": f"VD Trafikhændelse {uid[:8]}",
            "unique_id": object_id,
            "state_topic": state_topic,
            "json_attributes_topic": attr_topic,
            "icon": "mdi:alert-octagon-outline",
            "device": {
                "identifiers": ["vd_traffic_bridge"],
                "name": "Vejdirektoratet Trafikhændelser",
                "manufacturer": "Vejdirektoratet",
                "model": "Dataudveksler AMQP",
            },
        }
        self.client.publish(config_topic, json.dumps(payload), retain=True)

    def publish_event(self, event: TrafficEvent):
        self._ensure_discovery(event)
        uid = event.unique_id
        state_topic = f"{self.base_topic}/{uid}/state"
        attr_topic = f"{self.base_topic}/{uid}/attributes"

        state = "active" if event.is_active else "closed"
        attributes = {
            "situation_id": event.situation_id,
            "record_id": event.record_id,
            "record_type": event.record_type,
            "version": event.version,
            "creation_time": event.creation_time,
            "version_time": event.version_time,
            "start_time": event.start_time,
            "end_time": event.end_time,
            "comment": event.comment,
            "latitude": event.latitude,
            "longitude": event.longitude,
        }
        self.client.publish(state_topic, state, retain=True)
        self.client.publish(attr_topic, json.dumps(attributes), retain=True)
        log.info(
            "Publicerede hændelse %s (%s) status=%s",
            uid, event.record_type, state,
        )


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
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    dump_raw_xml = bool(cfg.get("logging", {}).get("dump_raw_xml", False))

    az = cfg["azure"]
    credential = ClientSecretCredential(
        tenant_id=az["tenant_id"],
        client_id=az["client_id"],
        client_secret=az["client_secret"],
    )

    publisher = HaMqttPublisher(cfg["mqtt"])
    publisher.connect()

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
                    for msg in receiver:
                        if _shutdown:
                            break
                        try:
                            body = b"".join(msg.body) if hasattr(msg, "body") else bytes(msg)
                            if dump_raw_xml:
                                log.debug("Rå DATEX II XML:\n%s", body.decode("utf-8", "replace"))
                            events = parse_datex_message(body)
                            if not events:
                                log.warning(
                                    "Ingen hændelser fundet i besked — sæt "
                                    "dump_raw_xml: true i opsætningen for at "
                                    "undersøge strukturen."
                                )
                            for event in events:
                                publisher.publish_event(event)
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

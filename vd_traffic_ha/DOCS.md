# Dokumentation — VD Trafikhændelser

## Opsætning

1. Installér add-on'et fra dit tilføjede repository
2. Gå til fanen **Configuration** og udfyld:

   **Azure (servicekonto)**
   - `tenant_id` — din Azure AD tenant-id
   - `client_id` — din servicekontos ID
   - `client_secret` — din servicekontos kode
   - `fully_qualified_namespace`, `topic_name`, `subscription_name` —
     hentes fra din AMQP-url på formen
     `amqps://<namespace>/<topic>/<subscription>`

   **MQTT**
   - `host` / `port` / `username` / `password` — din MQTT-brokers oplysninger
   - `base_topic` — forstavelse på alle state/attribute-topics (standard `vejdirektoratet/traffic`)
   - `discovery_prefix` — skal matche discovery-prefixet i din HA MQTT-integration (som regel `homeassistant`)

   **Logging**
   - `level` — `debug`/`info`/`warning`/`error`
   - `dump_raw_xml` — sæt til `true` midlertidigt for at se den fulde DATEX II XML for hver besked i loggen

3. Start add-on'et og hold øje med fanen **Log**

## Hvad add-on'et gør

- Autentificerer mod Azure AD med servicekontoens client credentials
- Holder en AMQP-forbindelse åben til den subscription, du har oprettet på Dataudveksleren
- Modtager DATEX II 3.2 XML-beskeder løbende (push, ikke polling)
- Parser hver besked og udtrækker situations-/record-id, type,
  oprettelses-/versionstidspunkt, start-/sluttidspunkt, beskrivelse og
  koordinater (hvis til stede)
- Publicerer hver hændelse til MQTT med tilhørende Home Assistant
  discovery-besked, så sensorer dukker automatisk op

## Vigtigt om XML-parsing

Feltmapningen er baseret på den generelle DATEX II 3.2-struktur og er
ikke testet mod en rigtig besked fra jeres feed. Hvis felter mangler
eller ser forkerte ud: sæt `dump_raw_xml: true`, kør add-on'et, og
send eksempler på den rå XML videre, så justeres parseren.

## Sensorer og kort

- **Én samlet sensor** (`sensor.vd_traffikhaendelser`) for alle hændelser
  — state er antal aktive hændelser, og alle detaljer (per hændelse,
  med alle DATEX II-underrecords) ligger som en liste i attributten
  `active_events`. Det undgår at der ophobes én permanent entitet pr.
  hændelse nogensinde (som den tidligere model gjorde).
- Koordinater publiceres som **tekst i fuld original præcision** i
  attributterne, så Home Assistants frontend ikke afrunder dem ved
  visning.
- Aktive hændelser med kendte koordinater vises desuden automatisk på
  Home Assistants **indbyggede kort-dashboard** (Oversigt → Kort, eller
  en Map-widget) — de oprettes som `device_tracker`-entiteter og
  fjernes automatisk fra HA igen når hændelsen lukkes (selv-oprensende,
  vokser ikke over tid).

## Kendte begrænsninger (v0.4.0)

- Ingen geografisk filtrering — alle hændelser fra feedet indgår i
  sammendraget. Med mange samtidige landsdækkende hændelser kan
  `active_events`-attributten blive stor; områdefiltrering (når den
  tilføjes) vil naturligt gøre listen mindre og mere relevant
- State genoprettes ikke fra en gemt tilstand ved genstart — kun nye
  beskeder fra feedet efter start
- `device_tracker`-tilgangen til kortvisning er en pragmatisk løsning
  (der findes ikke en MQTT-discovery-type til generelle geografiske
  markører) — virker fint til formålet, men entiteterne optræder
  teknisk set som "trackere" i HA's entitetsliste

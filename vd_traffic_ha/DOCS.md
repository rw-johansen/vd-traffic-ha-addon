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

## Kendte begrænsninger (v0.1.0)

- Ingen geografisk filtrering — alle hændelser fra feedet publiceres
- Ingen automatisk oprydning af sensorer for hændelser, der har været
  lukkede i lang tid
- State genoprettes ikke fra en gemt tilstand ved genstart — kun nye
  beskeder fra feedet efter start

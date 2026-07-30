# vd-traffic-ha-addon

Home Assistant add-on repository for **VD Trafikhændelser** — modtager
Vejdirektoratets trafikhændelser (dataset 415, "Traffic Events and
Road Works") i realtid via Azure Service Bus AMQP, parser DATEX II
3.2-beskederne, og publicerer dem til MQTT med Home Assistant
auto-discovery.

## Tilføj som repository i Home Assistant

Indstillinger → Add-ons → Add-on Store → ⋮ (øverst til højre) →
Repositories → indsæt:

```
https://github.com/rw-johansen/vd-traffic-ha-addon
```

"VD Trafikhændelser" dukker herefter op i Store-listen under
Local add-ons.

Se `vd_traffic_ha/DOCS.md` for opsætning og konfiguration.

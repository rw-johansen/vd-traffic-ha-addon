# VD Trafikhændelser

Modtager Vejdirektoratets trafikhændelser (dataset 415, "Traffic
Events and Road Works") i realtid via Azure Service Bus AMQP, parser
DATEX II 3.2-beskederne, og publicerer dem til MQTT med Home Assistant
auto-discovery.

Ingen geografisk filtrering endnu — alle hændelser fra feedet vises
som sensorer. Se `DOCS.md` for opsætning og detaljer.

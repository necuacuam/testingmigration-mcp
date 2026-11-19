package tools

// Prueba de punta a punta contra un Mongo real: arma el pipeline como lo
// haria analizar_mensajes, lo ejecuta y revisa lo leido. Usa una base
// temporal (testing_mcp_pruebas) que crea y borra. Se salta si no esta
// TESTING_MCP_PRUEBAS_URI.

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"testing-mcp/internal/conocimiento"
	"testing-mcp/internal/guard"
	"testing-mcp/internal/store"
)

const basePruebas = "testing_mcp_pruebas"

// envolver arma el texto como queda guardado en message_body: con \n y \"
// literales, como en el mensaje real de GrupoCliente.
func envolver(cuerpo string) string {
	return `<?xml version='1.0' encoding='UTF-8'?>\n<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">\n  <soap:Body>\n    ` +
		cuerpo + `\n  </soap:Body>\n</soap:Envelope>`
}

func preparar(t *testing.T) (Config, *guard.Guard) {
	t.Helper()
	uri := os.Getenv("TESTING_MCP_PRUEBAS_URI")
	if uri == "" {
		t.Skip("sin TESTING_MCP_PRUEBAS_URI")
	}
	ctx := context.Background()
	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	db := cli.Database(basePruebas)
	_ = db.Drop(ctx)
	t.Cleanup(func() { _ = db.Drop(context.Background()); _ = cli.Disconnect(context.Background()) })

	fechas := map[string]time.Time{
		"g1": time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC),
		"g2": time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC),
		"g3": time.Date(2026, 10, 1, 21, 30, 0, 0, time.UTC), // 23:30 en Madrid: sigue siendo el 1
		"g4": time.Date(2026, 10, 2, 8, 0, 0, 0, time.UTC),
		"g5": time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC),
		"g6": time.Date(2026, 10, 2, 10, 0, 0, 0, time.UTC),
	}
	gc := func(id, cuerpo string, extra bson.M) bson.M {
		d := bson.M{"correlation_id": id, "flow": "GrupoCliente-flow", "pais": "858", "message_body": envolver(cuerpo), "event_timestamp": fechas[id]}
		for k, v := range extra {
			d[k] = v
		}
		return d
	}
	ns := `xmlns=\"http://www.exampleURI.com/WSDL-COMERCIAL-VIGIA-Output\"`
	docs := []interface{}{
		// 1. el mensaje real: Codigo numerico, sin CodigoNormalizado
		gc("g1", `<GrupoClienteRequest `+ns+`>\n      <Operacion>UPSERT</Operacion>\n      <Pais>858</Pais>\n      <Codigo>1001</Codigo>\n      <Nombre>EMPRESA DEMO URUGUAY S.A                            </Nombre>\n      <Secuencial>100016</Secuencial>\n    </GrupoClienteRequest>`, nil),
		// 2. con prefijo en los campos, Codigo no numerico y CodigoNormalizado con valor
		gc("g2", `<ns0:GrupoClienteRequest xmlns:ns0=\"x\"><ns0:Codigo>AB12</ns0:Codigo><ns0:Secuencial>1</ns0:Secuencial><ns0:CodigoNormalizado>UC-1</ns0:CodigoNormalizado></ns0:GrupoClienteRequest>`, nil),
		// 3. CodigoNormalizado vacio con etiqueta autocerrada; Codigo no numerico
		gc("g3", `<GrupoClienteRequest><Codigo>X-9</Codigo><CodigoNormalizado/></GrupoClienteRequest>`, nil),
		// 4. CodigoNormalizado solo espacios; Codigo en CDATA
		gc("g4", `<GrupoClienteRequest><Codigo><![CDATA[C<1]]></Codigo><CodigoNormalizado>   </CodigoNormalizado></GrupoClienteRequest>`, nil),
		// 5. Codigo con un CDATA sin cerrar: ilegible
		gc("g5", `<GrupoClienteRequest><Codigo><![CDATA[77</Codigo></GrupoClienteRequest>`, nil),
		// 6. mock, no numerico y con CodigoNormalizado
		gc("g6", `<GrupoClienteRequest><Codigo>ZZ</Codigo><CodigoNormalizado>UC-6</CodigoNormalizado></GrupoClienteRequest>`, bson.M{"mock": true}),
		// 7. Cliente: Nombre solo dentro de ListadoDireccion (no hay Nombre de primer nivel)
		bson.M{"correlation_id": "c1", "flow": "Cliente-flow", "pais": "724", "message_body": envolver(
			`<ClienteRequest><Codigo>1</Codigo><ListadoDireccion><Nombre>CALLE</Nombre><Valor>Mayor</Valor></ListadoDireccion><ListadoDireccion><Nombre>NUMERO</Nombre><Valor>3</Valor></ListadoDireccion></ClienteRequest>`)},
		// 8. Cliente: Nombre de primer nivel despues de la lista, con prefijo
		bson.M{"correlation_id": "c2", "flow": "Cliente-flow", "pais": "724", "message_body": envolver(
			`<ns0:ClienteRequest xmlns:ns0=\"x\"><ns0:Codigo>2</ns0:Codigo><ns0:ListadoDireccion><ns0:Nombre>CALLE</ns0:Nombre></ns0:ListadoDireccion><ns0:Nombre>EMPRESA SA</ns0:Nombre></ns0:ClienteRequest>`)},
		// 8b. Cliente: como el mensaje real que repite xmlns en cada
		// elemento; NIF vacio autocerrado con atributo.
		bson.M{"correlation_id": "c3", "flow": "Cliente-flow", "pais": "724", "message_body": `<?xml version="1.0" encoding="UTF-8"?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
<SOAP-ENV:Body>
<ClienteRequest xmlns="http://www.exampleURI.com/WSDL-COMERCIAL-VIGIA-Output">
  <Operacion xmlns="http://www.exampleURI.com/WSDL-COMERCIAL-VIGIA-Output">UPSERT</Operacion>
  <Codigo xmlns="http://www.exampleURI.com/WSDL-COMERCIAL-VIGIA-Output">0100200</Codigo>
  <ListadoDireccion xmlns="http://www.exampleURI.com/WSDL-COMERCIAL-VIGIA-Output"><Nombre xmlns="u">CALLE</Nombre></ListadoDireccion>
  <Nombre xmlns="http://www.exampleURI.com/WSDL-COMERCIAL-VIGIA-Output">EMPRESA DEMO ESPANA S.A  .</Nombre>
  <NIF xmlns="http://www.exampleURI.com/WSDL-COMERCIAL-VIGIA-Output"/>
  <CodigoNormalizado xmlns="http://www.exampleURI.com/WSDL-COMERCIAL-VIGIA-Output">ES0200000001</CodigoNormalizado>
</ClienteRequest>
</SOAP-ENV:Body>
</SOAP-ENV:Envelope>`},
		// 9. ProcesarEmpleado: listas cuyo nombre es el final de otros
		// elementos (Direccion / AtributoDireccion, Division / CodDivision /
		// TipoDivision); Nombre de primer nivel despues de la lista.
		bson.M{"correlation_id": "p1", "flow": "ProcesarEmpleado-flow", "pais": "724", "message_body": envolver(
			`<ProcesarEmpleadoRequest><Codigo>E1</Codigo><Direccion><AtributoDireccion><Nombre>CALLE</Nombre><Valor>X</Valor></AtributoDireccion><AtributoDireccion><Nombre>NUM</Nombre><Valor>1</Valor></AtributoDireccion></Direccion><Division><DivisionEmpleado><CodDivision>D1</CodDivision></DivisionEmpleado></Division><TipoDivision>TR</TipoDivision><Nombre>ANA</Nombre></ProcesarEmpleadoRequest>`)},
		// 10. igual, con prefijo, sin Nombre de primer nivel ni Division
		bson.M{"correlation_id": "p2", "flow": "ProcesarEmpleado-flow", "pais": "724", "message_body": envolver(
			`<ns0:ProcesarEmpleadoRequest xmlns:ns0=\"x\"><ns0:Direccion><ns0:AtributoDireccion><ns0:Nombre>CALLE</ns0:Nombre></ns0:AtributoDireccion></ns0:Direccion><ns0:TipoDivisionAnt>VI</ns0:TipoDivisionAnt></ns0:ProcesarEmpleadoRequest>`)},
	}
	if _, err := db.Collection("test_inputs").InsertMany(ctx, docs); err != nil {
		t.Fatal(err)
	}

	conoc, err := conocimiento.Cargar("../../conocimiento/flows.json")
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.New(ctx, uri, basePruebas)
	if err != nil {
		t.Fatal(err)
	}
	madrid, err := time.LoadLocation("Europe/Madrid")
	if err != nil {
		t.Fatal(err)
	}
	c := Config{Conocimiento: conoc, Ejecutor: st, LimitePantalla: 20, Timeout: 30 * time.Second, Zona: madrid}
	return c, guard.New([]string{Coleccion})
}

func analizar(t *testing.T, c Config, g *guard.Guard, a argsAnalizar) map[string]interface{} {
	t.Helper()
	f, _ := c.Conocimiento.Flow(a.Flow)
	p, err := ArmarAnalisis(c, g, f, a)
	if err != nil {
		t.Fatal(err)
	}
	docs, err := c.Ejecutor.Ejecutar(context.Background(), Coleccion, p, c.Timeout)
	if err != nil {
		t.Fatal(err)
	}
	r := ResumirAnalisis(docs, a, f, c.LimitePantalla)
	b, _ := json.Marshal(r)
	var out map[string]interface{}
	_ = json.Unmarshal(b, &out)
	return out
}

func porID(r map[string]interface{}) map[string]map[string]interface{} {
	out := map[string]map[string]interface{}{}
	for _, f := range r["filas"].([]interface{}) {
		m := f.(map[string]interface{})
		out[m["correlation_id"].(string)] = m
	}
	return out
}

func sub(m map[string]interface{}, ruta string) interface{} {
	var v interface{} = m
	for _, p := range strings.Split(ruta, ".") {
		mm, ok := v.(map[string]interface{})
		if !ok {
			return nil
		}
		v = mm[p]
	}
	return v
}

func mensajes(r map[string]interface{}) (total, reales, mock float64) {
	m := r["mensajes"].(map[string]interface{})
	return m["total"].(float64), m["reales"].(float64), m["mock"].(float64)
}

func TestLecturaGrupoCliente(t *testing.T) {
	c, g := preparar(t)
	r := analizar(t, c, g, argsAnalizar{Flow: "GrupoCliente-flow", Campos: []string{"Codigo", "CodigoNormalizado", "Nombre"}})

	if tot, re, mo := mensajes(r); tot != 6 || re != 5 || mo != 1 {
		t.Fatalf("mensajes = %v/%v/%v, se esperaban 6 en total, 5 reales y 1 mock", tot, re, mo)
	}
	f := porID(r)
	casos := []struct {
		id, ruta string
		want     interface{}
	}{
		{"g1", "campos.Codigo", "1001"},
		{"g1", "presencia.CodigoNormalizado", "ausente"},
		{"g1", "campos.Nombre", "EMPRESA DEMO URUGUAY S.A                            "},
		{"g1", "mock", false},
		{"g2", "campos.Codigo", "AB12"},
		{"g2", "campos.CodigoNormalizado", "UC-1"},
		{"g3", "presencia.CodigoNormalizado", "vacio"},
		{"g4", "campos.Codigo", "C<1"},
		{"g4", "presencia.CodigoNormalizado", "vacio"},
		{"g5", "presencia.Codigo", "ilegible"},
		{"g5", "campos.Codigo", nil},
		{"g6", "mock", true},
	}
	for _, cs := range casos {
		if got := sub(f[cs.id], cs.ruta); got != cs.want {
			t.Errorf("%s %s = %#v, se esperaba %#v", cs.id, cs.ruta, got, cs.want)
		}
	}
	// El mensaje completo esta disponible para mostrarlo.
	if b, _ := f["g1"]["message_body"].(string); !strings.Contains(b, "<Codigo>1001</Codigo>") {
		t.Errorf("message_body de g1 = %q", b)
	}
	if _, hay := f["g1"]["pais"]; hay {
		t.Errorf("la fila trae campos del documento que no son del mensaje: %v", f["g1"])
	}
	lect := r["lectura"].(map[string]interface{})["Codigo"].(map[string]interface{})
	if lect["con_valor"].(float64) != 5 || lect["ilegible"].(float64) != 1 {
		t.Errorf("lectura Codigo = %v", lect)
	}
	avisos, _ := json.Marshal(r["avisos"])
	if !strings.Contains(string(avisos), "no se pudieron leer") {
		t.Errorf("faltaba el aviso de ilegibles: %s", avisos)
	}
}

// La pregunta que motivo el MCP: Codigo no numerico y CodigoNormalizado con valor.
func TestPreguntaCodigoNoNumerico(t *testing.T) {
	c, g := preparar(t)
	pregunta := []json.RawMessage{
		json.RawMessage(`{"$match":{"presencia.Codigo":"con_valor"}}`),
		json.RawMessage(`{"$group":{"_id":null,"noNumerico":{"$sum":{"$cond":[{"$regexMatch":{"input":"$campos.Codigo","regex":"^[0-9]+$"}},0,1]}},"caso":{"$sum":{"$cond":[{"$and":[{"$not":[{"$regexMatch":{"input":"$campos.Codigo","regex":"^[0-9]+$"}}]},{"$eq":["$presencia.CodigoNormalizado","con_valor"]}]},1,0]}}}}`),
	}
	casos := []struct {
		origen      string
		noNum, caso float64
	}{
		{OrigenReales, 3, 1}, // g2, g3, g4 no numericos; solo g2 con normalizado
		{OrigenMock, 1, 1},   // g6
		{OrigenTodos, 4, 2},
	}
	for _, cs := range casos {
		r := analizar(t, c, g, argsAnalizar{Flow: "GrupoCliente-flow", Campos: []string{"Codigo", "CodigoNormalizado"}, Origen: cs.origen, Pipeline: pregunta})
		fila := r["filas"].([]interface{})[0].(map[string]interface{})
		if fila["noNumerico"].(float64) != cs.noNum || fila["caso"].(float64) != cs.caso {
			t.Errorf("origen %s: %v, se esperaba noNumerico %v y caso %v", cs.origen, fila, cs.noNum, cs.caso)
		}
	}
}

func TestClienteListas(t *testing.T) {
	c, g := preparar(t)
	r := analizar(t, c, g, argsAnalizar{Flow: "Cliente-flow", Campos: []string{"Nombre", "ListadoDireccion"}})
	f := porID(r)
	if got := sub(f["c1"], "presencia.Nombre"); got != "ausente" {
		t.Errorf("c1 Nombre = %v: no debe leer el Nombre de ListadoDireccion", got)
	}
	if got := sub(f["c1"], "campos.ListadoDireccion"); got != float64(2) {
		t.Errorf("c1 ListadoDireccion = %v, se esperaba 2", got)
	}
	if got := sub(f["c2"], "campos.Nombre"); got != "EMPRESA SA" {
		t.Errorf("c2 Nombre = %v", got)
	}
	if got := sub(f["c2"], "campos.ListadoDireccion"); got != float64(1) {
		t.Errorf("c2 ListadoDireccion = %v, se esperaba 1", got)
	}
}

func TestElementosConAtributos(t *testing.T) {
	c, g := preparar(t)
	r := analizar(t, c, g, argsAnalizar{Flow: "Cliente-flow", Campos: []string{"Operacion", "Codigo", "Nombre", "NIF", "CodigoNormalizado", "ListadoDireccion"}})
	f := porID(r)
	casos := []struct {
		ruta string
		want interface{}
	}{
		{"campos.Operacion", "UPSERT"},
		{"campos.Codigo", "0100200"},
		{"campos.Nombre", "EMPRESA DEMO ESPANA S.A  ."},
		{"presencia.NIF", "vacio"},
		{"campos.CodigoNormalizado", "ES0200000001"},
		{"campos.ListadoDireccion", float64(1)},
	}
	for _, cs := range casos {
		if got := sub(f["c3"], cs.ruta); got != cs.want {
			t.Errorf("c3 %s = %#v, se esperaba %#v", cs.ruta, got, cs.want)
		}
	}
	if avisos, _ := json.Marshal(r["avisos"]); strings.Contains(string(avisos), "no se pudieron leer") {
		t.Errorf("no deberia haber ilegibles: %s", avisos)
	}
}

func TestListasConNombreQueTerminaIgual(t *testing.T) {
	c, g := preparar(t)
	r := analizar(t, c, g, argsAnalizar{Flow: "ProcesarEmpleado-flow", Campos: []string{"Nombre", "Direccion", "Division", "TipoDivision"}})
	f := porID(r)
	casos := []struct {
		id, ruta string
		want     interface{}
	}{
		{"p1", "campos.Nombre", "ANA"},
		{"p1", "campos.Direccion", float64(1)},
		{"p1", "campos.Division", float64(1)},
		{"p1", "campos.TipoDivision", "TR"},
		{"p2", "presencia.Nombre", "ausente"},
		{"p2", "campos.Direccion", float64(1)},
		{"p2", "presencia.Division", "ausente"},
		{"p2", "presencia.TipoDivision", "ausente"},
	}
	for _, cs := range casos {
		if got := sub(f[cs.id], cs.ruta); got != cs.want {
			t.Errorf("%s %s = %#v, se esperaba %#v", cs.id, cs.ruta, got, cs.want)
		}
	}
}

func TestPipelineConCampoNoPedido(t *testing.T) {
	c, g := preparar(t)
	f, _ := c.Conocimiento.Flow("GrupoCliente-flow")
	_, err := ArmarAnalisis(c, g, f, argsAnalizar{Flow: f.Flow, Campos: []string{"Codigo"},
		Pipeline: []json.RawMessage{json.RawMessage(`{"$match":{"presencia.CodigoNormalizado":"con_valor"}}`)}})
	if err == nil || !strings.Contains(err.Error(), "no esta en 'campos'") {
		t.Errorf("se esperaba rechazo, quedo %v", err)
	}
}

func TestPeriodoYAgrupacionPorDia(t *testing.T) {
	c, g := preparar(t)
	// Solo el 1 de octubre (hora de Madrid): g1, g2 y g3.
	r := analizar(t, c, g, argsAnalizar{Flow: "GrupoCliente-flow", Campos: []string{"Codigo"}, Desde: "2026-10-01", Hasta: "2026-10-01"})
	if tot, _, _ := mensajes(r); tot != 3 {
		t.Errorf("1 de octubre: %v mensajes, se esperaban 3", tot)
	}
	if got := sub(porID(r)["g1"], "event_timestamp"); got == nil {
		t.Errorf("la fila no trae event_timestamp")
	}
	// Con hora exacta: desde las 09:00 UTC del 2 (g5, g6).
	r = analizar(t, c, g, argsAnalizar{Flow: "GrupoCliente-flow", Campos: []string{"Codigo"}, Desde: "2026-10-02T09:00:00Z"})
	if tot, _, _ := mensajes(r); tot != 2 {
		t.Errorf("desde 2026-10-02T09:00Z: %v mensajes, se esperaban 2", tot)
	}
	// Agrupar por dia en el pipeline, en hora de Madrid.
	r = analizar(t, c, g, argsAnalizar{Flow: "GrupoCliente-flow", Campos: []string{"Codigo"}, Pipeline: []json.RawMessage{
		json.RawMessage(`{"$group":{"_id":{"$dateToString":{"format":"%Y-%m-%d","date":"$event_timestamp","timezone":"Europe/Madrid"}},"n":{"$sum":1}}}`),
		json.RawMessage(`{"$sort":{"_id":1}}`),
	}})
	b, _ := json.Marshal(r["filas"])
	if string(b) != `[{"_id":"2026-10-01","n":3},{"_id":"2026-10-02","n":3}]` {
		t.Errorf("por dia = %s", b)
	}
	f, _ := c.Conocimiento.Flow("GrupoCliente-flow")
	if _, err := ArmarAnalisis(c, g, f, argsAnalizar{Flow: f.Flow, Campos: []string{"Codigo"}, Desde: "01/10/2026"}); err == nil {
		t.Error("se esperaba error con una fecha mal escrita")
	}
}

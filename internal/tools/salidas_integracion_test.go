package tools

// Pruebas de punta a punta de las salidas (test_outputs) contra un Mongo
// real, sobre los mensajes de Cliente-flow de preparar (c1, c2, c3) y uno
// mas (c4). Se saltan si no esta TESTING_MCP_PRUEBAS_URI.

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
)

func prepararSalidas(t *testing.T) (Config, *guard.Guard) {
	t.Helper()
	c, g := preparar(t)
	ctx := context.Background()
	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("TESTING_MCP_PRUEBAS_URI")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Disconnect(context.Background()) })
	db := cli.Database(basePruebas)
	h := time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)
	if _, err := db.Collection("test_inputs").InsertOne(ctx, bson.M{"correlation_id": "c4", "flow": "Cliente-flow", "pais": "724",
		"message_body": envolver(`<ClienteRequest><Codigo>4</Codigo></ClienteRequest>`), "event_timestamp": h}); err != nil {
		t.Fatal(err)
	}
	sal := func(id, destino, cuerpo string, min int) bson.M {
		return bson.M{"correlation_id": id, "destino": destino, "flow": "Cliente-flow", "pais": "724", "url": "http://destino/" + destino,
			"message_body": cuerpo, "event_timestamp": h.Add(time.Duration(min) * time.Minute)}
	}
	docs := []interface{}{
		// c1 a POPS: las listas van antes que el Nombre de primer nivel,
		// para que una busqueda ingenua lea el de adentro.
		sal("c1", "POPS", `{
  "InputParameter": {
    "Operacion": "UPSERT",
    "Secuencial": 369012,
    "Estado": "Activado"
  },
  "Pais": "724",
  "DireccionList": [
    {"Nombre": "CALLE", "Valor": "Mayor"},
    {"Nombre": "NUMERO", "Valor": "3"}
  ],
  "AtributosClientesList": [],
  "Codigo": "1",
  "Nombre": "EMPRESA UNO",
  "InstitucionFinanciera": true,
  "DiaCierre": 15
}`, 1),
		// c1 a VIGIA: XML con prefijo, atributos y un Nombre dentro de
		// ListadoDireccion y de AtributoCliente antes del de primer nivel.
		sal("c1", "VIGIA", `<?xml version="1.0" encoding="UTF-8"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><wsdl:ClienteRequest xmlns:wsdl="x"><wsdl:Codigo>1</wsdl:Codigo><wsdl:ListadoDireccion><wsdl:Nombre>CALLE</wsdl:Nombre></wsdl:ListadoDireccion><wsdl:ListadoAtributosClientes><wsdl:AtributoCliente><wsdl:Nombre>A</wsdl:Nombre></wsdl:AtributoCliente><wsdl:AtributoCliente><wsdl:Nombre>B</wsdl:Nombre></wsdl:AtributoCliente></wsdl:ListadoAtributosClientes><wsdl:Nombre xmlns:a="b">EMPRESA UNO</wsdl:Nombre></wsdl:ClienteRequest></soapenv:Body></soapenv:Envelope>`, 2),
		// c1: una salida truncada, ademas de las dos completas
		sal("c1", "", `<?xml version="1.0"?><wsdl:ClienteRequest><wsdl:Codigo>1`, 3),
		// c2: solo una truncada
		sal("c2", "", `{"InputParameter": {"Operacion": "UPS`, 1),
		// c3 a POPS: guardado escapado; Nombre null
		sal("c3", "POPS", `{\n  \"InputParameter\": {\n    \"Operacion\": \"DELETE\"\n  },\n  \"Codigo\": \"0100200\",\n  \"Nombre\": null\n}`, 1),
		// c4 a POPS: JSON cortado dentro de InputParameter
		sal("c4", "POPS", `{"InputParameter": {"Operacion": "UPSERT", "Secuencial": 1`, 1),
	}
	if _, err := db.Collection("test_outputs").InsertMany(ctx, docs); err != nil {
		t.Fatal(err)
	}
	return c, g
}

func analizarSalidas(t *testing.T, c Config, g *guard.Guard, a argsSalidas) map[string]interface{} {
	t.Helper()
	p, info, err := ArmarSalidas(c, g, a)
	if err != nil {
		t.Fatal(err)
	}
	docs, err := c.Ejecutor.Ejecutar(context.Background(), Coleccion, p, c.Timeout)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(ResumirSalidas(docs, a, info, c.LimitePantalla))
	var out map[string]interface{}
	_ = json.Unmarshal(b, &out)
	return out
}

func TestSalidaJSON(t *testing.T) {
	c, g := prepararSalidas(t)
	r := analizarSalidas(t, c, g, argsSalidas{Flow: "Cliente-flow", Destino: "POPS", Campos: []string{"Codigo"},
		CamposSalida: []string{"InputParameter.Operacion", "InputParameter.Secuencial", "Codigo", "Nombre", "DireccionList", "AtributosClientesList", "InstitucionFinanciera", "DiaCierre"}})
	m, _ := json.Marshal(r["mensajes"])
	if string(m) != `{"con_salida":3,"mock":0,"reales":4,"sin_salida":1,"sin_salida_con_truncada":1,"total":4}` {
		t.Errorf("mensajes = %s", m)
	}
	f := porID(r)
	casos := []struct {
		id, ruta string
		want     interface{}
	}{
		{"c1", "campos_salida.InputParameter.Operacion", "UPSERT"},
		{"c1", "campos_salida.InputParameter.Secuencial", "369012"},
		{"c1", "campos_salida.Codigo", "1"},
		{"c1", "campos.Codigo", "1"},
		{"c1", "campos_salida.Nombre", "EMPRESA UNO"},
		{"c1", "campos_salida.DireccionList", float64(2)},
		{"c1", "presencia_salida.AtributosClientesList", "ausente"},
		{"c1", "campos_salida.InstitucionFinanciera", "true"},
		{"c1", "campos_salida.DiaCierre", "15"},
		{"c1", "salida.destino", "POPS"},
		{"c1", "salida_truncada", true},
		{"c2", "salida", nil},
		{"c2", "salida_truncada", true},
		{"c2", "presencia_salida.Nombre", nil},
		{"c3", "campos_salida.InputParameter.Operacion", "DELETE"},
		{"c3", "presencia_salida.InputParameter.Secuencial", "ausente"},
		{"c3", "campos_salida.Codigo", "0100200"},
		{"c3", "presencia_salida.Nombre", "vacio"},
		{"c3", "presencia_salida.DireccionList", "ausente"},
		{"c3", "salida_truncada", false},
		{"c4", "presencia_salida.InputParameter.Operacion", "ilegible"},
		{"c4", "campos_salida.InputParameter.Operacion", nil},
		{"c4", "presencia_salida.Codigo", "ausente"},
	}
	for _, cs := range casos {
		if got := sub(f[cs.id], cs.ruta); got != cs.want {
			t.Errorf("%s %s = %#v, se esperaba %#v", cs.id, cs.ruta, got, cs.want)
		}
	}
	l, _ := json.Marshal(r["lectura_salida"].(map[string]interface{})["InputParameter.Operacion"])
	if string(l) != `{"ausente":0,"con_valor":2,"ilegible":1,"vacio":0}` {
		t.Errorf("lectura_salida InputParameter.Operacion = %s", l)
	}
	avisos, _ := json.Marshal(r["avisos"])
	for _, a := range []string{"truncada", "no se pudieron leer"} {
		if !strings.Contains(string(avisos), a) {
			t.Errorf("falta el aviso %q: %s", a, avisos)
		}
	}
}

func TestSalidaXML(t *testing.T) {
	c, g := prepararSalidas(t)
	r := analizarSalidas(t, c, g, argsSalidas{Flow: "Cliente-flow", Destino: "VIGIA",
		CamposSalida: []string{"Codigo", "Nombre", "ListadoDireccion", "ListadoAtributosClientes.AtributoCliente", "ListadoGrupoCodigoSistema"}})
	f := porID(r)
	casos := []struct {
		id, ruta string
		want     interface{}
	}{
		{"c1", "campos_salida.Codigo", "1"},
		{"c1", "campos_salida.Nombre", "EMPRESA UNO"},
		{"c1", "campos_salida.ListadoDireccion", float64(1)},
		{"c1", "campos_salida.ListadoAtributosClientes.AtributoCliente", float64(2)},
		{"c1", "presencia_salida.ListadoGrupoCodigoSistema", "ausente"},
		{"c3", "salida", nil},
	}
	for _, cs := range casos {
		if got := sub(f[cs.id], cs.ruta); got != cs.want {
			t.Errorf("%s %s = %#v, se esperaba %#v", cs.id, cs.ruta, got, cs.want)
		}
	}
	if m := r["mensajes"].(map[string]interface{}); m["con_salida"].(float64) != 1 {
		t.Errorf("mensajes = %v", m)
	}
}

// Comparar un campo de la entrada con el de la salida en el pipeline.
func TestCompararEntradaYSalida(t *testing.T) {
	c, g := prepararSalidas(t)
	r := analizarSalidas(t, c, g, argsSalidas{Flow: "Cliente-flow", Destino: "POPS", Campos: []string{"Codigo"}, CamposSalida: []string{"Codigo"},
		Pipeline: []json.RawMessage{
			json.RawMessage(`{"$match":{"presencia_salida.Codigo":{"$ne":null}}}`),
			json.RawMessage(`{"$group":{"_id":{"$eq":["$campos.Codigo","$campos_salida.Codigo"]},"n":{"$sum":1}}}`),
			json.RawMessage(`{"$sort":{"_id":1}}`),
		}})
	// c1: 1 = 1; c3: 0100200 = 0100200; c4: 4 frente a ausente
	b, _ := json.Marshal(r["filas"])
	if string(b) != `[{"_id":false,"n":1},{"_id":true,"n":2}]` {
		t.Errorf("comparacion = %s", b)
	}
}

func TestSalidasValidacion(t *testing.T) {
	c, g := prepararSalidas(t)
	casos := []struct {
		a    argsSalidas
		want string
	}{
		{argsSalidas{Flow: "Cliente-flow", Destino: "POPS", CamposSalida: []string{"Pais"},
			Pipeline: []json.RawMessage{json.RawMessage(`{"$match":{"campos_salida.Codigo":"1"}}`)}}, "no esta en 'campos_salida'"},
		{argsSalidas{Flow: "Cliente-flow", Destino: "POPS", CamposSalida: []string{"DireccionList.Nombre"}}, "es una lista"},
		{argsSalidas{Flow: "Cliente-flow", Destino: "POPS", CamposSalida: []string{"NoExiste"}}, "no existe"},
		{argsSalidas{Flow: "Cliente-flow", Destino: "PROFATTECNO", CamposSalida: []string{"Codigo"}}, "no esta entre las salidas conocidas"},
		{argsSalidas{Flow: "Cliente-flow", Destino: ""}, "falta destino"},
		{argsSalidas{Flow: "Cliente-flow", Destino: "POPS",
			Pipeline: []json.RawMessage{json.RawMessage(`{"$lookup":{"from":"otra","localField":"a","foreignField":"b","as":"c"}}`)}}, "no es una coleccion permitida"},
	}
	for _, cs := range casos {
		if _, _, err := ArmarSalidas(c, g, cs.a); err == nil || !strings.Contains(err.Error(), cs.want) {
			t.Errorf("%+v: se esperaba %q, quedo %v", cs.a, cs.want, err)
		}
	}
	// un tramo de una ruta pedida si se puede usar; un destino desconocido
	// sin campos_salida, tambien
	if _, _, err := ArmarSalidas(c, g, argsSalidas{Flow: "Cliente-flow", Destino: "POPS", CamposSalida: []string{"InputParameter.Secuencial"},
		Pipeline: []json.RawMessage{json.RawMessage(`{"$project":{"x":"$campos_salida.InputParameter"}}`)}}); err != nil {
		t.Error(err)
	}
	r := analizarSalidas(t, c, g, argsSalidas{Flow: "Cliente-flow", Destino: "PROFATTECNO"})
	if avisos, _ := json.Marshal(r["avisos"]); !strings.Contains(string(avisos), "no se conoce su formato") {
		t.Errorf("avisos = %s", avisos)
	}
}

func TestSeguirMensaje(t *testing.T) {
	c, _ := prepararSalidas(t)
	ent, sal := PipelinesSeguir("c1")
	e, err := c.Ejecutor.Ejecutar(context.Background(), Coleccion, ent, c.Timeout)
	if err != nil {
		t.Fatal(err)
	}
	s, err := c.Ejecutor.Ejecutar(context.Background(), ColeccionSalidas, sal, c.Timeout)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(ResumirSeguir(c, "c1", e, s))
	var r map[string]interface{}
	_ = json.Unmarshal(b, &r)
	if sub(r, "entrada.flow") != "Cliente-flow" {
		t.Errorf("entrada = %v", r["entrada"])
	}
	var orden []string
	for _, x := range r["salidas"].([]interface{}) {
		m := x.(map[string]interface{})
		orden = append(orden, m["destino"].(string)+"/"+strings.TrimSpace(func() string { f, _ := m["formato"].(string); return f }()))
	}
	if strings.Join(orden, ",") != "POPS/json,VIGIA/xml,/" {
		t.Errorf("salidas = %v", orden)
	}
	tiene := map[string]bool{}
	for _, x := range r["destinosDelFlow"].([]interface{}) {
		m := x.(map[string]interface{})
		tiene[m["destino"].(string)] = m["tieneSalida"].(bool)
	}
	if !tiene["POPS"] || !tiene["VIGIA"] || tiene["PROFAT"] {
		t.Errorf("destinosDelFlow = %v", tiene)
	}
	if avisos, _ := json.Marshal(r["avisos"]); !strings.Contains(string(avisos), "destino vacio") {
		t.Errorf("avisos = %s", avisos)
	}
	// sin entrada
	b, _ = json.Marshal(ResumirSeguir(c, "x", nil, nil))
	if !strings.Contains(string(b), "no hay entrada") || !strings.Contains(string(b), "no hay salidas") {
		t.Errorf("sin datos = %s", b)
	}
}

func TestAnalizarConSalidas(t *testing.T) {
	c, g := prepararSalidas(t)
	r := analizar(t, c, g, argsAnalizar{Flow: "Cliente-flow", Campos: []string{"Codigo"}, ConSalidas: true})
	m, _ := json.Marshal(r["mensajes"])
	if string(m) != `{"con_salida":4,"mock":0,"reales":4,"sin_salida":0,"total":4}` {
		t.Errorf("mensajes = %s", m)
	}
	f := porID(r)
	if n := len(f["c1"]["salidas"].([]interface{})); n != 3 {
		t.Errorf("c1 tiene %d salidas, se esperaban 3", n)
	}
	for _, x := range f["c2"]["salidas"].([]interface{}) {
		if x.(map[string]interface{})["truncada"] != true {
			t.Errorf("c2: %v", x)
		}
	}
	if _, hay := f["c1"]["salidas"].([]interface{})[0].(map[string]interface{})["message_body"]; hay {
		t.Error("las salidas resumidas no deben traer el cuerpo")
	}
}

// Todas las rutas legibles de todas las salidas arman un pipeline que Mongo
// acepta (las expresiones se validan aunque no haya mensajes del flow).
func TestTodasLasRutasSeEjecutan(t *testing.T) {
	c, g := prepararSalidas(t)
	var rutas func(cs []conocimiento.CampoSalida, pre string, json bool) []string
	rutas = func(cs []conocimiento.CampoSalida, pre string, json bool) []string {
		var out []string
		for _, x := range cs {
			r := pre + x.Nombre
			plano := true
			for _, h := range x.Campos {
				plano = plano && h.Forma == conocimiento.FormaValor
			}
			// de un objeto se piden sus campos (el objeto solo se cuenta en
			// TestSalidaXML: no se puede pedir junto con sus campos)
			switch {
			case x.Forma == conocimiento.FormaObjeto:
				out = append(out, rutas(x.Campos, r+".", json)...)
			case x.Forma != conocimiento.FormaLista || !json || plano:
				out = append(out, r)
			}
		}
		return out
	}
	total, mayor := 0, 0
	for _, f := range c.Salidas.Flows {
		for _, s := range f.Salidas {
			if len(s.Destinos) == 0 {
				continue
			}
			rs := rutas(s.Campos, "", s.Formato == conocimiento.FormatoJSON)
			for len(rs) > 0 {
				n := 15
				if n > len(rs) {
					n = len(rs)
				}
				a := argsSalidas{Flow: f.Flow, Destino: s.Destinos[0].Destino, CamposSalida: rs[:n]}
				rs = rs[n:]
				p, _, err := ArmarSalidas(c, g, a)
				if err != nil {
					t.Errorf("%s %s: %v", f.Flow, s.Subflow, err)
					continue
				}
				b, _ := bson.Marshal(bson.D{{Key: "p", Value: p}})
				if len(b) > mayor {
					mayor = len(b)
				}
				if _, err := c.Ejecutor.Ejecutar(context.Background(), Coleccion, p, c.Timeout); err != nil {
					t.Errorf("%s %s %v: %v", f.Flow, s.Subflow, a.CamposSalida, err)
				}
				total += n
			}
		}
	}
	t.Logf("%d rutas; pipeline mas grande %d bytes", total, mayor)
}

func TestObjetoYSusCamposALaVez(t *testing.T) {
	c, g := prepararSalidas(t)
	_, _, err := ArmarSalidas(c, g, argsSalidas{Flow: "Cliente-flow", Destino: "POPS", CamposSalida: []string{"InputParameter", "InputParameter.Secuencial"}})
	if err == nil || !strings.Contains(err.Error(), "a la vez") {
		t.Errorf("se esperaba rechazo, quedo %v", err)
	}
}

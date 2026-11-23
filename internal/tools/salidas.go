package tools

// Herramientas sobre las salidas (test_outputs): lo que Mule envio a cada
// destino por cada mensaje de entrada, unido por correlation_id.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"go.mongodb.org/mongo-driver/bson"

	"testing-mcp/internal/conocimiento"
	"testing-mcp/internal/guard"
	"testing-mcp/internal/xmlcampos"
)

// comoLeerSalidas explica lo que agrega analizar_salidas a cada mensaje.
const comoLeerSalidas = "analizar_salidas parte de los mensajes de entrada del flow y busca, por correlation_id, la salida al destino pedido. Deja de cada mensaje: correlation_id, mock, event_timestamp y message_body de la entrada; " +
	"salida (null si no hubo salida a ese destino; si no, {destino, event_timestamp, message_body}); salida_truncada (true si el mensaje tiene una salida con destino vacio: el log la corto); " +
	"presencia.<campo> y campos.<campo> de los campos de entrada pedidos (igual que analizar_mensajes); " +
	"presencia_salida.<ruta> y campos_salida.<ruta> de los campos de salida pedidos (null si no hubo salida). " +
	"Un campo de salida se pide por su ruta con puntos desde el elemento raiz del mensaje, como lo muestra diccionario: Codigo, InputParameter.Secuencial. Los tramos intermedios tienen que ser objetos; una lista se cuenta (cuantas veces viene), no se lee por dentro. " +
	"En JSON, campos_salida.<ruta> es el texto tal como esta en el mensaje: sin comillas en los textos, y los numeros y booleanos como texto (369012, true); null cuenta como vacio. " +
	"Para comparar entrada y salida, usar el origen y la expresion de cada campo en diccionario: un campo puede cambiar de nombre (origen) o de valor (expresion), y con skipNullOn los campos vacios no se envian."

// todosLosFlows son los flows con entrada conocida y, al final, los que
// solo tienen salidas.
func todosLosFlows(c Config) []string {
	out := c.Conocimiento.NombresFlows()
	for _, n := range c.Salidas.NombresFlows() {
		if !contiene(out, n) {
			out = append(out, n)
		}
	}
	return out
}

func contiene(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

// agregarAlGrupo suma acumuladores a la etapa $group de una lectura.
func agregarAlGrupo(grupo bson.D, extra ...bson.E) {
	g := grupo[0].Value.(bson.D)
	grupo[0].Value = append(g, extra...)
}

// buscarSalidas trae, en __out, todas las salidas del mensaje (por el
// indice correlation_id + destino de test_outputs).
func buscarSalidas() bson.D {
	return bson.D{{Key: "$lookup", Value: bson.D{
		{Key: "from", Value: ColeccionSalidas},
		{Key: "localField", Value: "correlation_id"},
		{Key: "foreignField", Value: "correlation_id"},
		{Key: "as", Value: "__out"},
	}}}
}

// etapasSalidasResumidas agregan a cada mensaje 'salidas': destino, fecha y
// si esta truncada, sin el cuerpo.
func etapasSalidasResumidas() []bson.D {
	return []bson.D{
		buscarSalidas(),
		{{Key: "$addFields", Value: bson.D{{Key: "salidas", Value: bson.D{{Key: "$map", Value: bson.D{
			{Key: "input", Value: "$__out"},
			{Key: "in", Value: bson.D{
				{Key: "destino", Value: "$$this.destino"},
				{Key: "event_timestamp", Value: "$$this.event_timestamp"},
				{Key: "truncada", Value: bson.D{{Key: "$eq", Value: bson.A{"$$this.destino", ""}}}},
			}},
		}}}}}}},
		{{Key: "$unset", Value: "__out"}},
	}
}

// --- seguir_mensaje ---

func herramientaSeguir(c Config) mcp.Tool {
	return mcp.NewTool("seguir_mensaje", append(soloLectura(),
		mcp.WithDescription("Sigue un mensaje por su correlation_id: devuelve la entrada (test_inputs: flow, pais, mock, event_timestamp, message_body) y todas las salidas que Mule envio por ella (test_outputs: destino, event_timestamp, url, message_body), ordenadas por hora, con el formato y el sub-flow de cada una segun diccionario. "+
			"Tambien lista los destinos que el flow tiene en la configuracion de produccion, con sus configuraciones (es-pro, uy-pro...), y si este mensaje tiene salida a cada uno. Una salida con destino vacio esta truncada (el log la corto)."),
		mcp.WithString("correlation_id", mcp.Required(), mcp.Description("correlation_id del mensaje.")),
	)...)
}

func manejarSeguir(c Config) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var a struct {
			CorrelationID string `json:"correlation_id"`
		}
		if err := req.BindArguments(&a); err != nil {
			return mcp.NewToolResultError("argumentos invalidos: " + err.Error()), nil
		}
		id := strings.TrimSpace(a.CorrelationID)
		if id == "" {
			return mcp.NewToolResultError("falta correlation_id"), nil
		}
		ent, sal := PipelinesSeguir(id)
		entradas, err := c.Ejecutor.Ejecutar(ctx, Coleccion, ent, c.Timeout)
		if err != nil {
			return mcp.NewToolResultError("error consultando test_inputs: " + err.Error()), nil
		}
		salidas, err := c.Ejecutor.Ejecutar(ctx, ColeccionSalidas, sal, c.Timeout)
		if err != nil {
			return mcp.NewToolResultError("error consultando test_outputs: " + err.Error()), nil
		}
		return resultadoJSON(ResumirSeguir(c, id, entradas, salidas))
	}
}

// PipelinesSeguir arma las dos consultas: la entrada y sus salidas.
func PipelinesSeguir(id string) (entrada, salidas []bson.D) {
	mock := bson.D{{Key: "$eq", Value: bson.A{"$mock", true}}}
	entrada = []bson.D{
		{{Key: "$match", Value: bson.D{{Key: "correlation_id", Value: id}}}},
		{{Key: "$project", Value: bson.D{{Key: "_id", Value: 0}, {Key: "correlation_id", Value: 1}, {Key: "flow", Value: 1}, {Key: "pais", Value: 1},
			{Key: "mock", Value: mock}, {Key: "event_timestamp", Value: 1}, {Key: "message_body", Value: 1}}}},
		{{Key: "$limit", Value: 2}},
	}
	salidas = []bson.D{
		{{Key: "$match", Value: bson.D{{Key: "correlation_id", Value: id}}}},
		{{Key: "$sort", Value: bson.D{{Key: "event_timestamp", Value: 1}, {Key: "destino", Value: 1}}}},
		{{Key: "$project", Value: bson.D{{Key: "_id", Value: 0}, {Key: "destino", Value: 1}, {Key: "flow", Value: 1}, {Key: "pais", Value: 1},
			{Key: "url", Value: 1}, {Key: "mock", Value: mock}, {Key: "event_timestamp", Value: 1}, {Key: "message_body", Value: 1}}}},
		{{Key: "$limit", Value: 100}},
	}
	return entrada, salidas
}

// ResumirSeguir arma la respuesta de seguir_mensaje.
func ResumirSeguir(c Config, id string, entradas, salidas []map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{"correlation_id": id}
	var avisos []string
	flow := ""
	if len(entradas) > 0 {
		out["entrada"] = entradas[0]
		flow, _ = entradas[0]["flow"].(string)
		if len(entradas) > 1 {
			avisos = append(avisos, "hay mas de una entrada con este correlation_id: se muestra la primera")
		}
	} else {
		out["entrada"] = nil
		avisos = append(avisos, "no hay entrada (test_inputs) con este correlation_id")
	}
	if flow == "" && len(salidas) > 0 {
		flow, _ = salidas[0]["flow"].(string)
	}
	fs, conocido := c.Salidas.Flow(flow)
	con := map[string]bool{}
	truncadas := 0
	for _, s := range salidas {
		d, _ := s["destino"].(string)
		if d == "" {
			s["truncada"] = true
			truncadas++
			continue
		}
		con[d] = true
		if x, ok := fs.PorDestino(d); ok {
			s["formato"] = x.Formato
			s["subflow"] = x.Subflow
		} else if conocido {
			avisos = append(avisos, fmt.Sprintf("el destino %s no esta en la configuracion de produccion de %s que se uso para diccionario: no se conoce el formato de esa salida", d, flow))
		}
	}
	if salidas == nil {
		salidas = []map[string]interface{}{}
	}
	out["salidas"] = salidas
	if len(salidas) == 100 {
		avisos = append(avisos, "se muestran las primeras 100 salidas")
	}
	if truncadas > 0 {
		avisos = append(avisos, fmt.Sprintf("%d salidas con destino vacio: el log las corto antes del destino, el message_body esta incompleto", truncadas))
	}
	if flow != "" {
		var destinos []map[string]interface{}
		for _, x := range fs.Salidas {
			for _, d := range x.Destinos {
				destinos = append(destinos, map[string]interface{}{
					"destino": d.Destino, "configuraciones": d.Configuraciones, "subflow": x.Subflow, "tieneSalida": con[d.Destino],
				})
			}
		}
		if destinos == nil {
			destinos = []map[string]interface{}{}
		}
		out["destinosDelFlow"] = destinos
		if !conocido {
			avisos = append(avisos, fmt.Sprintf("el flow %s no tiene salidas en diccionario (Mule no loguea lo que envia)", flow))
		}
	}
	if len(salidas) == 0 {
		avisos = append(avisos, "no hay salidas (test_outputs) con este correlation_id")
	}
	if len(avisos) > 0 {
		out["avisos"] = avisos
	}
	return out
}

// --- analizar_salidas ---

func herramientaSalidas(c Config) mcp.Tool {
	l := fmt.Sprint(c.LimitePantalla)
	return mcp.NewTool("analizar_salidas", append(soloLectura(),
		mcp.WithDescription("Une cada mensaje de entrada de un flow con su salida a un destino (por correlation_id), lee en el servidor los campos pedidos de la salida (XML o JSON) y, si se piden, de la entrada, y ejecuta sobre el resultado las etapas de 'pipeline'. Sirve para ver que mensajes no tuvieron salida a un destino y para comparar un campo de la entrada con el que se envio. "+comoLeerSalidas+" "+
			"Devuelve 'mensajes' (total, reales, mock, con_salida, sin_salida y sin_salida_con_truncada), 'lectura' (por campo de entrada) y 'lectura_salida' (por campo de salida, sobre los mensajes con salida) con cuantos lo tienen con valor, vacio, ausente o ilegible, y 'filas' (como maximo "+l+", con aviso si hay mas)."),
		mcp.WithString("flow", mcp.Required(), mcp.Enum(todosLosFlows(c)...), mcp.Description("Flow de entrada.")),
		mcp.WithString("destino", mcp.Required(), mcp.Description("Destino tal como se guarda en test_outputs.destino (VIGIA, POPS, PROFAT...). Los del flow estan en diccionario; uno que no este ahi se acepta, pero sin campos_salida (no se conoce su formato).")),
		mcp.WithArray("campos_salida", mcp.Items(map[string]any{"type": "string"}),
			mcp.Description("Opcional. Campos de la salida a leer, por su ruta con puntos (Codigo, InputParameter.Secuencial). En 'pipeline' solo se pueden usar los pedidos aqui.")),
		mcp.WithArray("campos", mcp.Items(map[string]any{"type": "string"}),
			mcp.Description("Opcional. Campos del XML de entrada a leer, tal como los nombra diccionario. En 'pipeline' solo se pueden usar los pedidos aqui.")),
		mcp.WithString("origen", mcp.Enum(OrigenTodos, OrigenReales, OrigenMock),
			mcp.Description("Que mensajes analizar: todos (por defecto), solo los reales o solo los de mock.")),
		mcp.WithString("desde", mcp.Description("Opcional. Como en analizar_mensajes: por la fecha de la entrada.")),
		mcp.WithString("hasta", mcp.Description("Opcional. Como en analizar_mensajes: por la fecha de la entrada.")),
		mcp.WithArray("pipeline", mcp.Items(map[string]any{"type": "object"}),
			mcp.Description("Opcional. Etapas de agregacion Mongo (Extended JSON) que se aplican despues de leer, sobre correlation_id, mock, event_timestamp, message_body, salida, salida_truncada, presencia.<campo>, campos.<campo>, presencia_salida.<ruta> y campos_salida.<ruta>. Solo etapas de lectura, sin JavaScript y sin $facet.")),
	)...)
}

type argsSalidas struct {
	Flow         string            `json:"flow"`
	Destino      string            `json:"destino"`
	CamposSalida []string          `json:"campos_salida"`
	Campos       []string          `json:"campos"`
	Origen       string            `json:"origen"`
	Desde        string            `json:"desde"`
	Hasta        string            `json:"hasta"`
	Pipeline     []json.RawMessage `json:"pipeline"`
}

func manejarSalidas(c Config, g *guard.Guard) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var a argsSalidas
		if err := req.BindArguments(&a); err != nil {
			return mcp.NewToolResultError("argumentos invalidos: " + err.Error()), nil
		}
		pipeline, info, err := ArmarSalidas(c, g, a)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		docs, err := c.Ejecutor.Ejecutar(ctx, Coleccion, pipeline, c.Timeout)
		if err != nil {
			return mcp.NewToolResultError("error ejecutando la consulta (puede haberse agotado el tiempo maximo de " + c.Timeout.String() + "): " + err.Error()), nil
		}
		return resultadoJSON(ResumirSalidas(docs, a, info, c.LimitePantalla))
	}
}

// InfoSalidas es lo que ArmarSalidas resolvio y ResumirSalidas necesita.
type InfoSalidas struct {
	Entrada  *conocimiento.Flow
	Salida   *conocimiento.Salida
	Rutas    []xmlcampos.RutaSalida
	Conocido []string // destinos conocidos del flow
}

// ArmarSalidas valida los argumentos y arma el pipeline completo.
func ArmarSalidas(c Config, g *guard.Guard, a argsSalidas) ([]bson.D, InfoSalidas, error) {
	var info InfoSalidas
	if !contiene(todosLosFlows(c), a.Flow) {
		return nil, info, fmt.Errorf("flow %q no cubierto (ver diccionario)", a.Flow)
	}
	a.Destino = strings.TrimSpace(a.Destino)
	if a.Destino == "" {
		return nil, info, fmt.Errorf("falta destino; las salidas con destino vacio estan truncadas y no se leen (salen como salida_truncada)")
	}
	if f, ok := c.Conocimiento.Flow(a.Flow); ok {
		info.Entrada = &f
	} else if len(a.Campos) > 0 {
		return nil, info, fmt.Errorf("la entrada de %s no esta en flows.json: no se pueden pedir campos de entrada", a.Flow)
	}
	fs, _ := c.Salidas.Flow(a.Flow)
	info.Conocido = fs.Destinos()
	if s, ok := fs.PorDestino(a.Destino); ok {
		info.Salida = &s
	}
	if len(a.CamposSalida) > 0 {
		if info.Salida == nil {
			return nil, info, fmt.Errorf("el destino %q no esta entre las salidas conocidas de %s (%s): sin su formato no se pueden leer campos_salida", a.Destino, a.Flow, strings.Join(info.Conocido, ", "))
		}
		rutas, err := xmlcampos.ValidarRutas(*info.Salida, a.CamposSalida)
		if err != nil {
			return nil, info, err
		}
		info.Rutas = rutas
	}

	match, err := filtroEntrada(c, a.Flow, a.Origen, a.Desde, a.Hasta)
	if err != nil {
		return nil, info, err
	}
	pipeline := []bson.D{{{Key: "$match", Value: match}}}
	if len(a.Campos) > 0 {
		etapas, err := xmlcampos.Etapas(*info.Entrada, a.Campos)
		if err != nil {
			return nil, info, err
		}
		pipeline = append(pipeline, etapas...)
	} else {
		pipeline = append(pipeline, bson.D{{Key: "$project", Value: bson.D{
			{Key: "_id", Value: 0}, {Key: "correlation_id", Value: 1}, {Key: "mock", Value: bson.D{{Key: "$eq", Value: bson.A{"$mock", true}}}},
			{Key: "event_timestamp", Value: 1}, {Key: "message_body", Value: 1},
		}}})
	}

	alDestino := bson.D{{Key: "$filter", Value: bson.D{
		{Key: "input", Value: "$__out"},
		{Key: "cond", Value: bson.D{{Key: "$eq", Value: bson.A{"$$this.destino", a.Destino}}}},
	}}}
	pipeline = append(pipeline,
		buscarSalidas(),
		bson.D{{Key: "$addFields", Value: bson.D{
			{Key: "salida", Value: bson.D{{Key: "$let", Value: bson.D{
				{Key: "vars", Value: bson.D{{Key: "x", Value: bson.D{{Key: "$arrayElemAt", Value: bson.A{alDestino, 0}}}}}},
				{Key: "in", Value: bson.D{{Key: "$cond", Value: bson.A{
					bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: "$$x"}}, "object"}}},
					bson.D{{Key: "destino", Value: "$$x.destino"}, {Key: "event_timestamp", Value: "$$x.event_timestamp"}, {Key: "message_body", Value: "$$x.message_body"}},
					nil,
				}}}},
			}}}},
			{Key: "salida_truncada", Value: bson.D{{Key: "$in", Value: bson.A{"", bson.D{{Key: "$ifNull", Value: bson.A{"$__out.destino", bson.A{}}}}}}}},
		}}},
		bson.D{{Key: "$unset", Value: "__out"}},
	)
	if len(info.Rutas) > 0 {
		pipeline = append(pipeline, xmlcampos.EtapasSalida(*info.Salida, info.Rutas, "$salida.message_body")...)
	}

	pedidos := map[string]bool{}
	for _, n := range a.Campos {
		pedidos[n] = true
	}
	var rutas []string
	for _, r := range info.Rutas {
		rutas = append(rutas, r.Ruta)
	}
	if err := validarPipeline(g, a.Pipeline, refsEntrada(pedidos, rutas)); err != nil {
		return nil, info, err
	}
	filas, err := filasUsuario(c, a.Pipeline)
	if err != nil {
		return nil, info, err
	}

	conSalida := bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: "$salida"}}, "object"}}}
	suma := func(si interface{}) bson.D {
		return bson.D{{Key: "$sum", Value: bson.D{{Key: "$cond", Value: bson.A{si, 1, 0}}}}}
	}
	lectura := xmlcampos.Lectura(a.Campos)
	agregarAlGrupo(lectura,
		bson.E{Key: "con_salida", Value: suma(conSalida)},
		bson.E{Key: "sin_salida_con_truncada", Value: suma(bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "$not", Value: bson.A{conSalida}}}, "$salida_truncada"}}})},
	)
	agregarAlGrupo(lectura, xmlcampos.ConteosSalida(info.Rutas)...)
	pipeline = append(pipeline, bson.D{{Key: "$facet", Value: bson.D{
		{Key: "filas", Value: filas},
		{Key: "lectura", Value: bson.A{lectura}},
	}}})
	return pipeline, info, nil
}

// ResumirSalidas da forma al documento unico que devuelve el $facet.
func ResumirSalidas(docs []map[string]interface{}, a argsSalidas, info InfoSalidas, limite int) map[string]interface{} {
	origen := a.Origen
	if origen == "" {
		origen = OrigenTodos
	}
	out := map[string]interface{}{"flow": a.Flow, "destino": a.Destino, "origen": origen}
	if info.Salida != nil {
		out["subflow"] = info.Salida.Subflow
		out["formato"] = info.Salida.Formato
	}
	if a.Desde != "" || a.Hasta != "" {
		out["periodo"] = map[string]string{"desde": a.Desde, "hasta": a.Hasta}
	}
	var filas []interface{}
	lect := map[string]interface{}{}
	if len(docs) == 1 {
		filas, _ = docs[0]["filas"].([]interface{})
		if l, ok := docs[0]["lectura"].([]interface{}); ok && len(l) == 1 {
			lect, _ = l[0].(map[string]interface{})
		}
	}
	total, mock, con := numero(lect["total"]), numero(lect["mock"]), numero(lect["con_salida"])
	out["mensajes"] = map[string]int64{
		"total": total, "reales": total - mock, "mock": mock,
		"con_salida": con, "sin_salida": total - con, "sin_salida_con_truncada": numero(lect["sin_salida_con_truncada"]),
	}
	var avisos []string
	estados := []string{xmlcampos.ConValor, xmlcampos.Vacio, xmlcampos.Ausente, xmlcampos.Ilegible}

	entrada := map[string]interface{}{}
	for _, n := range a.Campos {
		if _, ya := entrada[n]; ya {
			continue
		}
		e := map[string]int64{}
		for _, st := range estados {
			e[st] = numero(lect[n+"__"+st])
		}
		entrada[n] = e
		if e[xmlcampos.Ilegible] > 0 {
			avisos = append(avisos, fmt.Sprintf("%s (entrada): %d mensajes no se pudieron leer; no cuentan en las otras cifras de ese campo", n, e[xmlcampos.Ilegible]))
		}
	}
	if len(a.Campos) > 0 {
		out["lectura"] = entrada
	}
	salida := map[string]interface{}{}
	for _, r := range info.Rutas {
		e := map[string]int64{}
		for _, st := range estados {
			e[st] = numero(lect[xmlcampos.ClaveSalida(r.Ruta, st)])
		}
		salida[r.Ruta] = e
		if e[xmlcampos.Ilegible] > 0 {
			avisos = append(avisos, fmt.Sprintf("%s (salida): %d mensajes no se pudieron leer; no cuentan en las otras cifras de ese campo", r.Ruta, e[xmlcampos.Ilegible]))
		}
		if c, ok := campoSalida(*info.Salida, r.Ruta); ok && c.NoEstaEnLaEntrada {
			avisos = append(avisos, fmt.Sprintf("%s: segun el DataWeave, su origen no es un campo del XSD de entrada (ver diccionario)", r.Ruta))
		}
	}
	if len(info.Rutas) > 0 {
		out["lectura_salida"] = salida
	}
	if info.Salida == nil {
		if len(info.Conocido) > 0 {
			avisos = append(avisos, fmt.Sprintf("el destino %s no esta entre las salidas conocidas de %s (%s): se ve si hubo salida, pero no se conoce su formato", a.Destino, a.Flow, strings.Join(info.Conocido, ", ")))
		} else {
			avisos = append(avisos, fmt.Sprintf("%s no tiene salidas conocidas en diccionario", a.Flow))
		}
	}
	if n := numero(lect["sin_salida_con_truncada"]); n > 0 {
		avisos = append(avisos, fmt.Sprintf("%d mensajes sin salida a %s tienen una salida truncada (destino vacio): puede ser la que falta", n, a.Destino))
	}
	if len(filas) > limite {
		filas = filas[:limite]
		avisos = append(avisos, fmt.Sprintf("'filas' recortado a %d (limite fijo del servidor): hay mas", limite))
	}
	if filas == nil {
		filas = []interface{}{}
	}
	out["filas"] = filas
	if total == 0 {
		avisos = append(avisos, "no hay mensajes de este flow con ese origen y periodo")
	} else if con == 0 {
		avisos = append(avisos, fmt.Sprintf("ningun mensaje tiene salida a %s", a.Destino))
	}
	if len(avisos) > 0 {
		out["avisos"] = avisos
	}
	return out
}

func campoSalida(s conocimiento.Salida, ruta string) (conocimiento.CampoSalida, bool) {
	nivel := s.Campos
	var c conocimiento.CampoSalida
	for _, p := range strings.Split(ruta, ".") {
		ok := false
		for _, x := range nivel {
			if x.Nombre == p {
				c, ok = x, true
				break
			}
		}
		if !ok {
			return c, false
		}
		nivel = c.Campos
	}
	return c, true
}

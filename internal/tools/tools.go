// Package tools registra las dos herramientas del servidor, que trabajan
// solo sobre los mensajes de entrada guardados en test_inputs.message_body:
//
//   - diccionario: formato del XML de entrada de cada flow (campos, tipos,
//     obligatorios, valores posibles). No consulta la base.
//   - analizar_mensajes: lee campos del XML de los mensajes de un flow en el
//     servidor (ver xmlcampos), aplica las etapas del modelo y devuelve
//     siempre, ademas, cuantos mensajes tienen cada campo con valor, vacio,
//     ausente o ilegible.
package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"go.mongodb.org/mongo-driver/bson"

	"testing-mcp/internal/conocimiento"
	"testing-mcp/internal/guard"
	"testing-mcp/internal/xmlcampos"
)

// Coleccion es la unica coleccion que se consulta.
const Coleccion = "test_inputs"

// Valores de origen.
const (
	OrigenTodos  = "todos"
	OrigenReales = "reales"
	OrigenMock   = "mock"
)

// Ejecutor corre un pipeline; lo implementa store.Store.
type Ejecutor interface {
	Ejecutar(ctx context.Context, coleccion string, pipeline []bson.D, timeout time.Duration) ([]map[string]interface{}, error)
}

type Config struct {
	Conocimiento   *conocimiento.Conocimiento
	Ejecutor       Ejecutor
	LimitePantalla int
	Timeout        time.Duration
	// Zona en que se interpreta una fecha sin hora en desde/hasta.
	Zona *time.Location
}

const Instrucciones = "Servidor MCP de solo lectura para estudiar el contenido de los mensajes de entrada de los flows de Mule (el XML que recibe cada flow). " +
	"Usar 'diccionario' para el formato del mensaje de un flow y para saber que campos existen: nunca inventar un flow ni un campo que no este ahi. " +
	"Usar 'analizar_mensajes' para cualquier pregunta sobre lo que traen los mensajes: el servidor lee los campos del XML y devuelve, ademas del resultado, cuantos mensajes tienen cada campo con valor, vacio, ausente o ilegible. " +
	"Hay mensajes reales y mensajes de mock (generados para pruebas): cada mensaje trae mock true/false, y 'origen' permite estudiar solo unos u otros. " +
	"Cada mensaje trae event_timestamp (cuando Mule lo recibio): para limitar el periodo usar 'desde' y 'hasta'; para agrupar por dia, mes u hora, usar event_timestamp en 'pipeline' ($dateToString o $dateTrunc, con la zona horaria que corresponda). " +
	"Al responder, decir siempre sobre cuantos mensajes se calculo, de que origen y periodo, y si algun campo salio ilegible."

func Registrar(s *server.MCPServer, c Config) {
	g := guard.New([]string{Coleccion})
	s.AddTool(herramientaDiccionario(c), manejarDiccionario(c))
	s.AddTool(herramientaAnalizar(c), manejarAnalizar(c, g))
}

func soloLectura() []mcp.ToolOption {
	return []mcp.ToolOption{
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
	}
}

// comoLeer explica lo que agrega analizar_mensajes a cada mensaje.
const comoLeer = "analizar_mensajes deja de cada mensaje: correlation_id, mock (true/false), event_timestamp (fecha y hora en que Mule recibio el mensaje, tipo date, en UTC), message_body (el XML completo, tal como se guardo), " +
	"presencia.<campo> (con_valor, vacio, ausente o ilegible; vacio = solo espacios o nada) y campos.<campo>. " +
	"En forma valor, campos.<campo> es el texto tal cual, sin recortar espacios, o null si esta ausente o ilegible. " +
	"En forma lista, es cuantas veces viene el elemento (0 si no viene, null si ilegible). " +
	"Para saber si un campo tiene valor, usar presencia.<campo> = con_valor, no comparar campos.<campo> con \"\". " +
	"Los campos se buscan por nombre, con o sin prefijo de namespace; el orden en el XML no importa."

// --- diccionario ---

func herramientaDiccionario(c Config) mcp.Tool {
	return mcp.NewTool("diccionario", append(soloLectura(),
		mcp.WithDescription("Devuelve el formato del mensaje de entrada de cada flow cubierto: elemento raiz y campos del XML con tipo, si es obligatorio, forma (valor o lista), valores posibles y notas. Sin 'flow', devuelve todos."),
		mcp.WithString("flow", mcp.Enum(append([]string{""}, c.Conocimiento.NombresFlows()...)...),
			mcp.Description("Flow a describir; vacio = todos.")),
	)...)
}

func manejarDiccionario(c Config) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var a struct {
			Flow string `json:"flow"`
		}
		if err := req.BindArguments(&a); err != nil {
			return mcp.NewToolResultError("argumentos invalidos: " + err.Error()), nil
		}
		out := map[string]interface{}{
			"descripcion":   c.Conocimiento.Descripcion,
			"comoLeerElXML": comoLeer,
		}
		if a.Flow == "" {
			out["flows"] = c.Conocimiento.Flows
		} else {
			f, ok := c.Conocimiento.Flow(a.Flow)
			if !ok {
				return mcp.NewToolResultError(fmt.Sprintf("flow %q no cubierto", a.Flow)), nil
			}
			out["flows"] = []conocimiento.Flow{f}
		}
		return resultadoJSON(out)
	}
}

// --- analizar_mensajes ---

func herramientaAnalizar(c Config) mcp.Tool {
	l := fmt.Sprint(c.LimitePantalla)
	return mcp.NewTool("analizar_mensajes", append(soloLectura(),
		mcp.WithDescription("Lee campos del XML de los mensajes de entrada de un flow, en el servidor, y ejecuta sobre el resultado las etapas de 'pipeline'. "+comoLeer+" "+
			"Devuelve 'mensajes' (cuantos se analizaron: total, reales y mock), 'lectura' (por campo pedido, cuantos mensajes lo tienen con valor, vacio, ausente o ilegible) y 'filas' (lo que devuelve 'pipeline', como maximo "+l+", con aviso si hay mas). "+
			"Sin 'pipeline', 'lectura' ya es el perfil de los campos pedidos."),
		mcp.WithString("flow", mcp.Required(), mcp.Enum(c.Conocimiento.NombresFlows()...), mcp.Description("Flow cuyos mensajes se analizan.")),
		mcp.WithArray("campos", mcp.Required(), mcp.Items(map[string]any{"type": "string"}),
			mcp.Description("Campos del XML a leer, tal como los nombra diccionario. En 'pipeline' solo se pueden usar los campos pedidos aqui.")),
		mcp.WithString("origen", mcp.Enum(OrigenTodos, OrigenReales, OrigenMock),
			mcp.Description("Que mensajes analizar: todos (por defecto), solo los reales o solo los de mock.")),
		mcp.WithString("desde", mcp.Description("Opcional. Solo mensajes recibidos desde este momento, inclusive. Fecha (2026-10-01: desde las 00:00 en "+c.Zona.String()+") o fecha y hora con zona (2026-10-01T08:00:00+02:00).")),
		mcp.WithString("hasta", mcp.Description("Opcional. Solo mensajes recibidos hasta este momento. Una fecha sola incluye el dia completo (2026-10-15: hasta el final del 15 en "+c.Zona.String()+"); con hora, es el instante exacto, excluido. Filtrar por fecha aqui y no en pipeline: asi mensajes y lectura cuentan solo ese periodo.")),
		mcp.WithArray("pipeline", mcp.Items(map[string]any{"type": "object"}),
			mcp.Description("Opcional. Etapas de agregacion Mongo (Extended JSON) que se aplican despues de leer el XML, sobre correlation_id, mock, event_timestamp, message_body, presencia.<campo> y campos.<campo>. Solo etapas de lectura, sin JavaScript y sin $facet.")),
	)...)
}

type argsAnalizar struct {
	Flow     string            `json:"flow"`
	Campos   []string          `json:"campos"`
	Origen   string            `json:"origen"`
	Desde    string            `json:"desde"`
	Hasta    string            `json:"hasta"`
	Pipeline []json.RawMessage `json:"pipeline"`
}

// refCampo encuentra referencias a campos.X o presencia.X en el pipeline.
var refCampo = regexp.MustCompile(`(?:campos|presencia)\.([A-Za-z0-9_]+)`)

func manejarAnalizar(c Config, g *guard.Guard) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var a argsAnalizar
		if err := req.BindArguments(&a); err != nil {
			return mcp.NewToolResultError("argumentos invalidos: " + err.Error()), nil
		}
		f, ok := c.Conocimiento.Flow(a.Flow)
		if !ok {
			return mcp.NewToolResultError(fmt.Sprintf("flow %q no cubierto (ver diccionario)", a.Flow)), nil
		}
		pipeline, err := ArmarAnalisis(c, g, f, a)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		docs, err := c.Ejecutor.Ejecutar(ctx, Coleccion, pipeline, c.Timeout)
		if err != nil {
			return mcp.NewToolResultError("error ejecutando la consulta (puede haberse agotado el tiempo maximo de " + c.Timeout.String() + "): " + err.Error()), nil
		}
		return resultadoJSON(ResumirAnalisis(docs, a, f, c.LimitePantalla))
	}
}

// ArmarAnalisis valida los argumentos y arma el pipeline completo.
func ArmarAnalisis(c Config, g *guard.Guard, f conocimiento.Flow, a argsAnalizar) ([]bson.D, error) {
	pedidos := map[string]bool{}
	for _, n := range a.Campos {
		pedidos[n] = true
	}
	match := bson.D{{Key: "flow", Value: f.Flow}}
	switch a.Origen {
	case "", OrigenTodos:
	case OrigenReales:
		match = append(match, bson.E{Key: "mock", Value: bson.D{{Key: "$ne", Value: true}}})
	case OrigenMock:
		match = append(match, bson.E{Key: "mock", Value: true})
	default:
		return nil, fmt.Errorf("origen %q invalido: todos, reales o mock", a.Origen)
	}
	rango := bson.D{}
	if a.Desde != "" {
		t, _, err := instante(a.Desde, c.Zona)
		if err != nil {
			return nil, fmt.Errorf("desde: %w", err)
		}
		rango = append(rango, bson.E{Key: "$gte", Value: t})
	}
	if a.Hasta != "" {
		t, soloFecha, err := instante(a.Hasta, c.Zona)
		if err != nil {
			return nil, fmt.Errorf("hasta: %w", err)
		}
		if soloFecha {
			t = t.AddDate(0, 0, 1)
		}
		rango = append(rango, bson.E{Key: "$lt", Value: t})
	}
	if len(rango) > 0 {
		match = append(match, bson.E{Key: "event_timestamp", Value: rango})
	}

	etapas, err := xmlcampos.Etapas(f, a.Campos)
	if err != nil {
		return nil, err
	}

	if err := g.Validar(a.Pipeline, guard.Opciones{SinFacet: true}); err != nil {
		return nil, fmt.Errorf("pipeline rechazado: %w", err)
	}
	for _, raw := range a.Pipeline {
		for _, m := range refCampo.FindAllSubmatch(raw, -1) {
			if !pedidos[string(m[1])] {
				return nil, fmt.Errorf("pipeline rechazado: usa %s, pero %q no esta en 'campos'", m[0], m[1])
			}
		}
	}
	usuario, err := desdeJSON(a.Pipeline)
	if err != nil {
		return nil, err
	}
	filas := bson.A{}
	for _, e := range usuario {
		filas = append(filas, e)
	}
	filas = append(filas, bson.D{{Key: "$limit", Value: c.LimitePantalla + 1}})

	pipeline := []bson.D{{{Key: "$match", Value: match}}}
	pipeline = append(pipeline, etapas...)
	pipeline = append(pipeline, bson.D{{Key: "$facet", Value: bson.D{
		{Key: "filas", Value: filas},
		{Key: "lectura", Value: bson.A{xmlcampos.Lectura(a.Campos)}},
	}}})
	return pipeline, nil
}

// ResumirAnalisis da forma al documento unico que devuelve el $facet.
func ResumirAnalisis(docs []map[string]interface{}, a argsAnalizar, f conocimiento.Flow, limite int) map[string]interface{} {
	origen := a.Origen
	if origen == "" {
		origen = OrigenTodos
	}
	out := map[string]interface{}{"flow": f.Flow, "origen": origen}
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
	total, mock := numero(lect["total"]), numero(lect["mock"])
	out["mensajes"] = map[string]int64{"total": total, "reales": total - mock, "mock": mock}

	porCampo := map[string]interface{}{}
	var avisos []string
	for _, n := range a.Campos {
		if _, ya := porCampo[n]; ya {
			continue
		}
		e := map[string]int64{}
		for _, st := range []string{xmlcampos.ConValor, xmlcampos.Vacio, xmlcampos.Ausente, xmlcampos.Ilegible} {
			e[st] = numero(lect[n+"__"+st])
		}
		porCampo[n] = e
		if e[xmlcampos.Ilegible] > 0 {
			avisos = append(avisos, fmt.Sprintf("%s: %d mensajes no se pudieron leer (etiqueta o CDATA sin cerrar, o estructura inesperada); no cuentan en las otras cifras de ese campo", n, e[xmlcampos.Ilegible]))
		}
		if c, ok := f.Campo(n); ok && c.Obligatorio && e[xmlcampos.Ausente] > 0 {
			avisos = append(avisos, fmt.Sprintf("%s es obligatorio en el XSD y falta en %d mensajes", n, e[xmlcampos.Ausente]))
		}
	}
	out["lectura"] = porCampo
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
	}
	if len(avisos) > 0 {
		out["avisos"] = avisos
	}
	return out
}

// --- comun ---

// instante interpreta una fecha (2026-10-01, a las 00:00 en zona) o una
// fecha y hora RFC 3339 (2026-10-01T08:00:00+02:00 o ...Z).
func instante(s string, zona *time.Location) (time.Time, bool, error) {
	s = strings.TrimSpace(s)
	if t, err := time.ParseInLocation("2006-01-02", s, zona); err == nil {
		return t, true, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, false, nil
	}
	return time.Time{}, false, fmt.Errorf("%q no es una fecha valida: usar 2026-10-01 o 2026-10-01T08:00:00+02:00", s)
}

func desdeJSON(pipeline []json.RawMessage) ([]bson.D, error) {
	out := make([]bson.D, len(pipeline))
	for i, raw := range pipeline {
		if err := bson.UnmarshalExtJSON(raw, false, &out[i]); err != nil {
			return nil, fmt.Errorf("etapa %d: Extended JSON invalido: %w", i, err)
		}
	}
	return out, nil
}

func numero(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	case map[string]interface{}:
		// Extended JSON relajado puede traer {"$numberLong": "..."}
		if s, ok := n["$numberLong"].(string); ok {
			var i int64
			fmt.Sscan(strings.TrimSpace(s), &i)
			return i
		}
	}
	return 0
}

func resultadoJSON(v interface{}) (*mcp.CallToolResult, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return mcp.NewToolResultError("no se pudo armar la respuesta: " + err.Error()), nil
	}
	return mcp.NewToolResultText(string(b)), nil
}

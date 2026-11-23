package xmlcampos

// Lectura de los mensajes de salida (test_outputs.message_body), que pueden
// ser XML (SOAP) o JSON (REST, por ejemplo POPS). El formato y los campos
// de cada uno salen de conocimiento/salidas.json.
//
// Un campo de salida se pide por su ruta con puntos desde el elemento raiz
// del mensaje: "Codigo", "InputParameter.Secuencial". Los tramos
// intermedios tienen que ser de forma objeto (vienen una vez); dentro de
// una lista no se lee campo por campo: se cuenta cuantas veces viene, igual
// que en la entrada.
//
// En cada nivel, antes de buscar un nombre se quitan los bloques hermanos
// (listas u objetos) que tienen dentro un campo con ese nombre: Nombre de
// primer nivel no se confunde con DireccionList.Nombre.
//
// JSON, con texto fijo y sin regex:
//   - la clave se busca como "Campo": (no hace falta el orden);
//   - un objeto o una lista se delimita contando llaves o corchetes: un
//     texto con llaves o corchetes dentro de un valor lo puede confundir;
//   - un valor de texto es lo que va entre comillas, tal cual (sin
//     desescapar ñ ni \"); un numero o booleano, el texto hasta la
//     siguiente coma, llave o corchete; null cuenta como vacio;
//   - si el cuerpo se guardo escapado (todas las claves como \"Campo\":),
//     primero se desescapa.

import (
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"

	"testing-mcp/internal/conocimiento"
)

const (
	salidaNormal = "__sn"
	sinSalida    = "__SIN_SALIDA__"
)

// RutaSalida es un campo de salida pedido, ya validado.
type RutaSalida struct {
	Ruta   string   // con puntos: InputParameter.Secuencial
	partes []string // InputParameter, Secuencial
	// niveles[i] son los campos del nivel en que se busca partes[i].
	niveles [][]conocimiento.CampoSalida
	hoja    conocimiento.CampoSalida
}

// ValidarRutas comprueba que cada ruta sea un campo legible de la salida.
func ValidarRutas(s conocimiento.Salida, rutas []string) ([]RutaSalida, error) {
	var out []RutaSalida
	vistas := map[string]bool{}
	for _, r := range rutas {
		if vistas[r] {
			continue
		}
		vistas[r] = true
		partes := strings.Split(r, ".")
		nivel := s.Campos
		rs := RutaSalida{Ruta: r, partes: partes}
		for i, p := range partes {
			c, ok := buscarCampo(nivel, p)
			if !ok {
				return nil, fmt.Errorf("el campo de salida %q no esta en el mensaje de %s (ver diccionario): %q no existe en ese nivel", r, s.Subflow, p)
			}
			rs.niveles = append(rs.niveles, nivel)
			if i < len(partes)-1 {
				switch c.Forma {
				case conocimiento.FormaObjeto:
				case conocimiento.FormaLista:
					return nil, fmt.Errorf("%q: %s es una lista; los campos de una lista no se leen uno por uno, se pide la lista (%s) y se cuenta cuantas veces viene", r, p, strings.Join(partes[:i+1], "."))
				default:
					return nil, fmt.Errorf("%q: %s es un valor, no tiene campos", r, p)
				}
				nivel = c.Campos
				continue
			}
			if c.Forma == conocimiento.FormaLista && s.Formato == conocimiento.FormatoJSON {
				for _, h := range c.Campos {
					if h.Forma != conocimiento.FormaValor {
						return nil, fmt.Errorf("%q: la lista tiene dentro otras listas u objetos y no se puede contar con seguridad", r)
					}
				}
			}
			rs.hoja = c
		}
		out = append(out, rs)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("hay que pedir al menos un campo de salida")
	}
	// campos_salida.X no puede ser a la vez un numero (cuantas veces viene
	// X) y un objeto con X.Y dentro
	for _, a := range out {
		for _, b := range out {
			if strings.HasPrefix(b.Ruta, a.Ruta+".") {
				return nil, fmt.Errorf("no se puede pedir %s y %s a la vez: pedir solo %s (si viene, sus campos tienen valor o vacio; si no, ausente)", a.Ruta, b.Ruta, b.Ruta)
			}
		}
	}
	return out, nil
}

func buscarCampo(cs []conocimiento.CampoSalida, n string) (conocimiento.CampoSalida, bool) {
	for _, c := range cs {
		if c.Nombre == n {
			return c, true
		}
	}
	return conocimiento.CampoSalida{}, false
}

// EtapasSalida agrega, por cada ruta, presencia_salida.<ruta> (con_valor,
// vacio, ausente o ilegible; null si el mensaje no tiene esa salida) y
// campos_salida.<ruta> (el texto, o cuantas veces viene si es lista u
// objeto). cuerpo es la expresion del texto de la salida, por ejemplo
// "$salida.message_body", que puede ser null.
func EtapasSalida(s conocimiento.Salida, rutas []RutaSalida, cuerpo string) []bson.D {
	json := s.Formato == conocimiento.FormatoJSON
	etapas := []bson.D{{{Key: "$addFields", Value: bson.D{{Key: salidaNormal,
		Value: cond(eq(bson.D{{Key: "$ifNull", Value: bson.A{cuerpo, nil}}}, nil), sinSalida, cuerpo)}}}}}
	if json {
		etapas = append(etapas, bson.D{{Key: "$addFields", Value: bson.D{{Key: salidaNormal, Value: desescaparJSON("$" + salidaNormal)}}}})
	} else {
		// atributos fuera de las etiquetas de todos los nombres que se usan
		nombres := []string{}
		for _, r := range rutas {
			for i, p := range r.partes {
				nombres = agregar(nombres, p)
				for _, h := range quitarAntes(r.niveles[i], p) {
					nombres = agregar(nombres, h)
				}
			}
		}
		for _, x := range nombres {
			for _, inicio := range []string{"<" + x, ":" + x} {
				etapas = append(etapas, bson.D{{Key: "$addFields", Value: bson.D{{Key: salidaNormal, Value: sinAtributos("$"+salidaNormal, inicio)}}}})
			}
		}
	}

	quitar := bson.A{salidaNormal}
	finales := bson.D{}
	for i, r := range rutas {
		ambito := "$" + salidaNormal
		for j, p := range r.partes {
			// texto del nivel, sin los hermanos que confunden
			limpio := fmt.Sprintf("__s%d_%d", i, j)
			quitar = append(quitar, limpio)
			etapas = append(etapas, bson.D{{Key: "$addFields", Value: bson.D{{Key: limpio, Value: ambito}}}})
			for _, h := range quitarAntes(r.niveles[j], p) {
				var e bson.D
				if json {
					e = quitarJSON("$"+limpio, h)
				} else {
					e = quitarBloque("$"+limpio, h)
				}
				etapas = append(etapas, bson.D{{Key: "$addFields", Value: bson.D{{Key: limpio, Value: e}}}})
			}
			if j < len(r.partes)-1 {
				sub := fmt.Sprintf("__o%d_%d", i, j)
				quitar = append(quitar, sub)
				var b bson.D
				if json {
					b = bloqueJSON("$"+limpio, p)
				} else {
					b = bloqueXML("$"+limpio, p)
				}
				etapas = append(etapas, bson.D{{Key: "$addFields", Value: bson.D{{Key: sub, Value: b}}}})
				ambito = "$" + sub
				continue
			}
			v := fmt.Sprintf("__v%d", i)
			quitar = append(quitar, v)
			var crudo bson.D
			valorHoja := r.hoja.Forma == conocimiento.FormaValor
			switch {
			case json && valorHoja:
				crudo = valorJSON("$"+limpio, p)
			case json:
				crudo = contarJSON("$"+limpio, p)
			case valorHoja:
				crudo = valor("$"+limpio, p)
			default:
				crudo = contar("$"+limpio, p)
			}
			etapas = append(etapas, bson.D{{Key: "$addFields", Value: bson.D{{Key: v, Value: cond(eq("$"+salidaNormal, sinSalida), sinSalida, crudo)}}}})
			ref := "$" + v
			var pres, val interface{}
			if valorHoja {
				pres = presenciaValor(ref)
				val = cond(enTexto(ref), ref, nil)
			} else {
				pres = cond(eq(ref, -1), Ilegible, cond(gt(ref, 0), ConValor, Ausente))
				val = cond(eq(ref, -1), nil, ref)
			}
			finales = append(finales,
				bson.E{Key: "presencia_salida." + r.Ruta, Value: cond(eq(ref, sinSalida), nil, pres)},
				bson.E{Key: "campos_salida." + r.Ruta, Value: cond(eq(ref, sinSalida), nil, val)},
			)
		}
	}
	etapas = append(etapas,
		bson.D{{Key: "$addFields", Value: finales}},
		bson.D{{Key: "$unset", Value: quitar}},
	)
	return etapas
}

// quitarAntes devuelve los hermanos (listas u objetos) de nivel que tienen
// dentro un campo llamado n.
func quitarAntes(nivel []conocimiento.CampoSalida, n string) []string {
	var out []string
	for _, c := range nivel {
		if c.Forma != conocimiento.FormaValor && c.Nombre != n && contiene(c.Hijos(), n) {
			out = append(out, c.Nombre)
		}
	}
	return out
}

func agregar(l []string, s string) []string {
	if contiene(l, s) {
		return l
	}
	return append(l, s)
}

// ConteosSalida devuelve, para el $group de la lectura, cuantos mensajes
// tienen cada campo de salida en cada estado. Las claves son
// "salida:<ruta>__<estado>" (con / en vez de puntos: $group no admite
// puntos en los nombres).
func ConteosSalida(rutas []RutaSalida) bson.D {
	g := bson.D{}
	for _, r := range rutas {
		for _, e := range []string{ConValor, Vacio, Ausente, Ilegible} {
			g = append(g, bson.E{Key: ClaveSalida(r.Ruta, e), Value: bson.D{{Key: "$sum", Value: cond(eq("$presencia_salida."+r.Ruta, e), 1, 0)}}})
		}
	}
	return g
}

// ClaveSalida es la clave de ConteosSalida para una ruta y un estado.
func ClaveSalida(ruta, estado string) string {
	return "salida:" + strings.ReplaceAll(ruta, ".", "/") + "__" + estado
}

// --- XML ---

// bloqueXML devuelve el contenido del primer elemento X de b (sin sus
// etiquetas), "" si viene autocerrado, la marca de ausente si no viene o
// la de ilegible si sus etiquetas no cuadran.
func bloqueXML(b, x string) bson.D {
	return cond(bson.D{{Key: "$in", Value: bson.A{b, bson.A{marcaMala, marcaNo, sinSalida}}}}, b,
		bson.D{{Key: "$let", Value: bson.D{
			{Key: "vars", Value: bson.D{{Key: "t", Value: bson.D{{Key: "$split", Value: bson.A{marcarEtiquetas(b, x), marcaEtiqueta}}}}}},
			{Key: "in", Value: cond(eq(bson.D{{Key: "$size", Value: "$$t"}}, 1),
				cond(bson.D{{Key: "$or", Value: bson.A{gte(idx(b, "<"+x+"/>"), 0), gte(idx(b, ":"+x+"/>"), 0)}}}, "", marcaNo),
				cond(eq(bson.D{{Key: "$mod", Value: bson.A{bson.D{{Key: "$size", Value: "$$t"}}, 2}}}, 1),
					bson.D{{Key: "$arrayElemAt", Value: bson.A{"$$t", 1}}},
					marcaMala),
			)},
		}}})
}

// --- JSON ---

// desescaparJSON deja el JSON normal si se guardo con todas las claves
// escapadas (\"Campo\":).
func desescaparJSON(b string) bson.D {
	partes := func(sep string) bson.D {
		return bson.D{{Key: "$size", Value: bson.D{{Key: "$split", Value: bson.A{b, sep}}}}}
	}
	var t interface{} = b
	for _, r := range [][2]string{{`\"`, `"`}, {`\n`, "\n"}, {`\r`, "\r"}, {`\t`, "\t"}} {
		t = reemplazar(t, r[0], r[1])
	}
	return cond(bson.D{{Key: "$and", Value: bson.A{
		gt(partes(`":`), 1),
		eq(partes(`":`), partes(`\":`)),
	}}}, t, b)
}

// restoJSON es el texto que sigue a la clave "n": desde la posicion pos,
// sin los espacios del principio.
func restoJSON(b interface{}, n string) bson.D {
	clave := `"` + n + `":`
	return bson.D{{Key: "$ltrim", Value: bson.D{
		{Key: "input", Value: bson.D{{Key: "$substrCP", Value: bson.A{b,
			bson.D{{Key: "$add", Value: bson.A{"$$pos", len([]rune(clave))}}},
			bson.D{{Key: "$strLenCP", Value: b}},
		}}}},
		{Key: "chars", Value: " \n\r\t"},
	}}}
}

// conClave evalua en con $$pos (posicion de "n":) y $$r (lo que sigue,
// sin espacios); si la clave no esta, devuelve siNo. Si b es una marca, la
// devuelve.
func conClave(b interface{}, n string, siNo, en interface{}) bson.D {
	return cond(bson.D{{Key: "$in", Value: bson.A{b, bson.A{marcaMala, marcaNo, sinSalida}}}}, b,
		bson.D{{Key: "$let", Value: bson.D{
			{Key: "vars", Value: bson.D{{Key: "pos", Value: bson.D{{Key: "$indexOfCP", Value: bson.A{b, `"` + n + `":`}}}}}},
			{Key: "in", Value: cond(lt("$$pos", 0), siNo, bson.D{{Key: "$let", Value: bson.D{
				{Key: "vars", Value: bson.D{{Key: "r", Value: restoJSON(b, n)}}},
				{Key: "in", Value: en},
			}}})},
		}}})
}

func empieza(s interface{}, pre string) bson.D {
	return eq(bson.D{{Key: "$substrCP", Value: bson.A{s, 0, len([]rune(pre))}}}, pre)
}

// cerrarJSON devuelve el objeto o la lista con que empieza r, con sus
// llaves o corchetes, o la marca de ilegible si no cierra. Cuenta las
// aperturas hasta cada cierre: el bloque termina en el primer cierre en que
// cuadran.
func cerrarJSON(r interface{}, abre, cierra string) bson.D {
	add := func(x ...interface{}) bson.D { return bson.D{{Key: "$add", Value: bson.A(x)}} }
	largo := func(x interface{}) bson.D { return bson.D{{Key: "$strLenCP", Value: x}} }
	aperturas := bson.D{{Key: "$subtract", Value: bson.A{
		bson.D{{Key: "$size", Value: bson.D{{Key: "$split", Value: bson.A{"$$this", abre}}}}}, 1}}}
	paso := bson.D{{Key: "$let", Value: bson.D{
		{Key: "vars", Value: bson.D{{Key: "a2", Value: add("$$value.a", aperturas)}}},
		{Key: "in", Value: bson.D{
			{Key: "a", Value: "$$a2"},
			{Key: "c", Value: add("$$value.c", 1)},
			{Key: "largo", Value: add("$$value.largo", largo("$$this"), 1)},
			{Key: "fin", Value: eq("$$a2", add("$$value.c", 1))},
		}},
	}}}
	recorrido := bson.D{{Key: "$reduce", Value: bson.D{
		{Key: "input", Value: bson.D{{Key: "$split", Value: bson.A{r, cierra}}}},
		{Key: "initialValue", Value: bson.D{{Key: "a", Value: 0}, {Key: "c", Value: 0}, {Key: "largo", Value: 0}, {Key: "fin", Value: false}}},
		{Key: "in", Value: cond("$$value.fin", "$$value", paso)},
	}}}
	// el ultimo trozo no va seguido de un cierre: si se llego ahi sin
	// cuadrar, no cierra
	cierraBien := bson.D{{Key: "$and", Value: bson.A{"$$e.fin", bson.D{{Key: "$lte", Value: bson.A{"$$e.largo", largo(r)}}}}}}
	return bson.D{{Key: "$let", Value: bson.D{
		{Key: "vars", Value: bson.D{{Key: "e", Value: recorrido}}},
		{Key: "in", Value: cond(cierraBien, bson.D{{Key: "$substrCP", Value: bson.A{r, 0, "$$e.largo"}}}, marcaMala)},
	}}}
}

// bloqueJSON devuelve el objeto que es el valor de la clave n (con sus
// llaves), la marca de ausente si no esta o es null, o la de ilegible.
func bloqueJSON(b, n string) bson.D {
	return conClave(b, n, marcaNo,
		cond(empieza("$$r", "{"), cerrarJSON("$$r", "{", "}"),
			cond(empieza("$$r", "null"), marcaNo, marcaMala)))
}

// quitarJSON quita de b la clave n y su valor (objeto o lista). Deja la
// coma: no importa para buscar otras claves.
func quitarJSON(b interface{}, n string) bson.D {
	return conClave(b, n, b, bson.D{{Key: "$let", Value: bson.D{
		{Key: "vars", Value: bson.D{{Key: "blq", Value: cond(empieza("$$r", "{"), cerrarJSON("$$r", "{", "}"),
			cond(empieza("$$r", "["), cerrarJSON("$$r", "[", "]"), ""))}}},
		{Key: "in", Value: cond(eq("$$blq", marcaMala), marcaMala, bson.D{{Key: "$concat", Value: bson.A{
			bson.D{{Key: "$substrCP", Value: bson.A{b, 0, "$$pos"}}},
			bson.D{{Key: "$substrCP", Value: bson.A{"$$r", bson.D{{Key: "$strLenCP", Value: "$$blq"}}, bson.D{{Key: "$strLenCP", Value: "$$r"}}}}},
		}}})},
	}}})
}

// valorJSON lee el valor de la clave n: el texto entre comillas, o un
// numero o booleano hasta la siguiente coma, llave, corchete o salto de
// linea; null es vacio.
func valorJSON(b, n string) bson.D {
	fin := func(sep string) bson.D {
		return bson.D{{Key: "$let", Value: bson.D{
			{Key: "vars", Value: bson.D{{Key: "k", Value: idx("$$r", sep)}}},
			{Key: "in", Value: cond(lt("$$k", 0), bson.D{{Key: "$strLenCP", Value: "$$r"}}, "$$k")},
		}}}
	}
	return cond(eq(b, marcaNo), marcaNo, conClave(b, n, marcaNo,
		cond(empieza("$$r", `"`), hasta("$$r", 1, `"`),
			cond(bson.D{{Key: "$or", Value: bson.A{empieza("$$r", "{"), empieza("$$r", "[")}}}, marcaMala,
				cond(empieza("$$r", "null"), "",
					trim(bson.D{{Key: "$substrCP", Value: bson.A{"$$r", 0, bson.D{{Key: "$min", Value: bson.A{
						fin(","), fin("}"), fin("]"), fin("\n"), fin("\r"),
					}}}}}}))))))
}

// contarJSON cuenta cuantas veces viene la clave n: los elementos de la
// lista (de objetos planos: cuantos "{" hay dentro), 1 si es un objeto, 0
// si no esta o es null, -1 si no se puede contar.
func contarJSON(b, n string) bson.D {
	return cond(eq(b, marcaMala), -1, cond(eq(b, marcaNo), 0, conClave(b, n, 0,
		cond(empieza("$$r", "["), bson.D{{Key: "$let", Value: bson.D{
			{Key: "vars", Value: bson.D{{Key: "l", Value: cerrarJSON("$$r", "[", "]")}}},
			{Key: "in", Value: cond(eq("$$l", marcaMala), -1,
				bson.D{{Key: "$subtract", Value: bson.A{bson.D{{Key: "$size", Value: bson.D{{Key: "$split", Value: bson.A{"$$l", "{"}}}}}, 1}}})},
		}}},
			cond(empieza("$$r", "{"), cond(eq(cerrarJSON("$$r", "{", "}"), marcaMala), -1, 1),
				cond(empieza("$$r", "null"), 0, -1))))))
}

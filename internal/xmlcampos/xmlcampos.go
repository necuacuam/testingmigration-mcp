// Package xmlcampos arma las etapas de agregacion que leen campos del XML
// guardado como texto en message_body, sin JavaScript y sin regex: solo
// $indexOfCP, $substrCP, $split y $reduce.
//
// Por que asi: un pipeline con $regexFind sobre message_body devolvio cero
// coincidencias en todos los mensajes sin dar error, mientras que la
// busqueda de texto fijo leyo el mismo campo en todos. Y un pipeline largo
// escrito a mano es facil de equivocar: lo arma siempre este paquete y cada
// resultado dice cuantos mensajes no se pudieron leer.
//
// Reglas de lectura de un campo de forma "valor" (F):
//   - primero se quitan los atributos de sus etiquetas: <F xmlns="..."> pasa
//     a <F> (hay mensajes validos que repiten xmlns en cada elemento);
//   - se busca la primera "<F>" o ":F>" (campo con o sin prefijo de
//     namespace); el valor es el texto hasta el siguiente "<", o el contenido
//     del CDATA si empieza con uno;
//   - "<F/>" o ":F/>" es un campo presente y vacio;
//   - antes de buscar, se quitan los bloques de forma "lista" que tienen un
//     hijo con el mismo nombre (por ejemplo, Nombre dentro de
//     ListadoDireccion), para no leer el de adentro.
//
// Un campo de forma "lista" se cuenta: cuantas veces viene el elemento.
//
// Se marca como ilegible lo que estas reglas no pueden leer con seguridad:
// una etiqueta o un CDATA sin cerrar, o un numero de etiquetas que no
// cuadra.
package xmlcampos

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"

	"testing-mcp/internal/conocimiento"
)

// Estados de presencia.X.
const (
	ConValor = "con_valor"
	Vacio    = "vacio"
	Ausente  = "ausente"
	Ilegible = "ilegible"
)

const (
	cuerpo    = "$message_body"
	normal    = "__n" // cuerpo sin atributos en las etiquetas que se leen
	limpio    = "__b" // normal sin los bloques que hay que quitar
	marcaMala = "__ILEGIBLE__"
	marcaNo   = "__NO__"
)

// Etapas devuelve las etapas que agregan, por cada campo pedido:
//   - presencia.<campo>: con_valor, vacio (solo espacios o nada), ausente o
//     ilegible;
//   - campos.<campo>: el texto tal cual (forma valor) o la cantidad de veces
//     que viene (forma lista).
//
// Al final deja de cada mensaje solo correlation_id, mock (true/false),
// event_timestamp, message_body, presencia y campos.
func Etapas(f conocimiento.Flow, nombres []string) ([]bson.D, error) {
	if len(nombres) == 0 {
		return nil, fmt.Errorf("hay que pedir al menos un campo")
	}
	var pedidos []conocimiento.Campo
	vistos := map[string]bool{}
	for _, n := range nombres {
		if vistos[n] {
			continue
		}
		vistos[n] = true
		c, ok := f.Campo(n)
		if !ok {
			return nil, fmt.Errorf("el campo %q no esta documentado en %s (ver diccionario)", n, f.Flow)
		}
		pedidos = append(pedidos, c)
	}

	// Bloques a quitar: listas con algun hijo que se llame como un campo
	// pedido de forma valor.
	porQuitar := []string{}
	for _, l := range f.Campos {
		if l.Forma != conocimiento.FormaLista {
			continue
		}
		for _, p := range pedidos {
			if p.Forma == conocimiento.FormaValor && contiene(l.Hijos, p.Nombre) {
				porQuitar = append(porQuitar, l.Nombre)
				break
			}
		}
	}

	// Primero se quitan los atributos de las etiquetas de los campos pedidos
	// y de las listas a quitar (<Codigo xmlns="..."> pasa a <Codigo>), una
	// etapa por etiqueta para que la expresion no crezca. Sobre ese texto se
	// cuentan las listas; sobre ese mismo texto sin los bloques, se leen los
	// valores.
	etapas := []bson.D{{{Key: "$addFields", Value: bson.D{{Key: normal, Value: cuerpo}}}}}
	porNormalizar := []string{}
	for _, p := range pedidos {
		porNormalizar = append(porNormalizar, p.Nombre)
	}
	for _, x := range porQuitar {
		if !contiene(porNormalizar, x) {
			porNormalizar = append(porNormalizar, x)
		}
	}
	for _, x := range porNormalizar {
		for _, inicio := range []string{"<" + x, ":" + x} {
			etapas = append(etapas, bson.D{{Key: "$addFields", Value: bson.D{{Key: normal, Value: sinAtributos("$"+normal, inicio)}}}})
		}
	}
	etapas = append(etapas, bson.D{{Key: "$addFields", Value: bson.D{{Key: limpio, Value: "$" + normal}}}})
	for _, x := range porQuitar {
		etapas = append(etapas, bson.D{{Key: "$addFields", Value: bson.D{{Key: limpio, Value: quitarBloque("$"+limpio, x)}}}})
	}

	// Primero los valores crudos en campos auxiliares; despues, con ellos,
	// presencia y campos.
	crudos := bson.D{}
	for _, p := range pedidos {
		if p.Forma == conocimiento.FormaValor {
			crudos = append(crudos, bson.E{Key: aux(p.Nombre), Value: valor("$"+limpio, p.Nombre)})
		} else {
			crudos = append(crudos, bson.E{Key: aux(p.Nombre), Value: contar("$"+normal, p.Nombre)})
		}
	}
	etapas = append(etapas, bson.D{{Key: "$addFields", Value: crudos}})

	presencia := bson.D{}
	campos := bson.D{}
	quitar := bson.A{limpio, normal}
	for _, p := range pedidos {
		v := "$" + aux(p.Nombre)
		quitar = append(quitar, aux(p.Nombre))
		switch {
		case p.Forma == conocimiento.FormaLista:
			presencia = append(presencia, bson.E{Key: p.Nombre, Value: cond(eq(v, -1), Ilegible, cond(gt(v, 0), ConValor, Ausente))})
			campos = append(campos, bson.E{Key: p.Nombre, Value: cond(eq(v, -1), nil, v)})
		default:
			presencia = append(presencia, bson.E{Key: p.Nombre, Value: presenciaValor(v)})
			campos = append(campos, bson.E{Key: p.Nombre, Value: cond(enTexto(v), v, nil)})
		}
	}
	etapas = append(etapas,
		bson.D{{Key: "$addFields", Value: bson.D{{Key: "presencia", Value: presencia}, {Key: "campos", Value: campos}}}},
		bson.D{{Key: "$unset", Value: quitar}},
		// Solo lo que interesa del mensaje: su identificador, si es de
		// mock, el texto y lo leido.
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "_id", Value: 0},
			{Key: "correlation_id", Value: 1},
			{Key: "mock", Value: eq("$mock", true)},
			{Key: "event_timestamp", Value: 1},
			{Key: "message_body", Value: 1},
			{Key: "presencia", Value: 1},
			{Key: "campos", Value: 1},
		}}},
	)
	return etapas, nil
}

// Lectura devuelve la etapa $group que cuenta los mensajes (total y de
// mock) y, por campo, cuantos hay en cada estado de presencia. Las claves
// son "<campo>__<estado>".
func Lectura(nombres []string) bson.D {
	g := bson.D{
		{Key: "_id", Value: nil},
		{Key: "total", Value: bson.D{{Key: "$sum", Value: 1}}},
		{Key: "mock", Value: bson.D{{Key: "$sum", Value: cond(eq("$mock", true), 1, 0)}}},
	}
	vistos := map[string]bool{}
	for _, n := range nombres {
		if vistos[n] {
			continue
		}
		vistos[n] = true
		for _, e := range []string{ConValor, Vacio, Ausente, Ilegible} {
			g = append(g, bson.E{Key: n + "__" + e, Value: bson.D{{Key: "$sum", Value: cond(eq("$presencia."+n, e), 1, 0)}}})
		}
	}
	return bson.D{{Key: "$group", Value: g}}
}

func aux(n string) string { return "__v_" + n }

// presenciaValor clasifica un valor crudo.
func presenciaValor(v string) bson.D {
	return cond(eq(v, marcaMala), Ilegible,
		cond(eq(v, marcaNo), Ausente,
			cond(eq(trim(v), ""), Vacio, ConValor)))
}

func enTexto(v string) bson.D {
	return bson.D{{Key: "$and", Value: bson.A{
		bson.D{{Key: "$ne", Value: bson.A{v, marcaMala}}},
		bson.D{{Key: "$ne", Value: bson.A{v, marcaNo}}},
	}}}
}

// valor lee el texto del primer campo F de primer nivel del texto b.
func valor(b, f string) bson.D {
	largo := len([]rune(f)) + 2 // "<F>" y ":F>" miden lo mismo
	return bson.D{{Key: "$let", Value: bson.D{
		{Key: "vars", Value: bson.D{
			{Key: "a", Value: idx(b, "<"+f+">")},
			{Key: "p", Value: idx(b, ":"+f+">")},
			{Key: "s", Value: bson.D{{Key: "$max", Value: bson.A{idx(b, "<"+f+"/>"), idx(b, ":"+f+"/>")}}}},
		}},
		{Key: "in", Value: bson.D{{Key: "$let", Value: bson.D{
			{Key: "vars", Value: bson.D{{Key: "i", Value: cond(lt("$$a", 0), "$$p", cond(lt("$$p", 0), "$$a", bson.D{{Key: "$min", Value: bson.A{"$$a", "$$p"}}}))}}},
			{Key: "in", Value: cond(eq(b, marcaMala), marcaMala,
				cond(lt("$$i", 0),
					cond(gte("$$s", 0), "", marcaNo),
					bson.D{{Key: "$let", Value: bson.D{
						{Key: "vars", Value: bson.D{{Key: "ini", Value: bson.D{{Key: "$add", Value: bson.A{"$$i", largo}}}}}},
						{Key: "in", Value: cond(
							eq(bson.D{{Key: "$substrCP", Value: bson.A{b, "$$ini", 9}}}, "<![CDATA["),
							hasta(b, bson.D{{Key: "$add", Value: bson.A{"$$ini", 9}}}, "]]>"),
							hasta(b, "$$ini", "<"),
						)},
					}}},
				),
			)},
		}}}},
	}}}
}

// hasta devuelve el texto de b desde ini hasta la primera aparicion de fin,
// o la marca de ilegible si fin no aparece.
func hasta(b string, ini interface{}, fin string) bson.D {
	return bson.D{{Key: "$let", Value: bson.D{
		{Key: "vars", Value: bson.D{{Key: "ini2", Value: ini}}},
		{Key: "in", Value: bson.D{{Key: "$let", Value: bson.D{
			{Key: "vars", Value: bson.D{{Key: "fin", Value: idxDesde(b, fin, "$$ini2")}}},
			{Key: "in", Value: cond(lt("$$fin", 0), marcaMala,
				bson.D{{Key: "$substrCP", Value: bson.A{b, "$$ini2", bson.D{{Key: "$subtract", Value: bson.A{"$$fin", "$$ini2"}}}}}})},
		}}}},
	}}}
}

// marcaEtiqueta reemplaza las etiquetas de un elemento para poder partir el
// texto por ellas. No aparece en un XML.
const marcaEtiqueta = "\x01"

// etiquetas son las tres formas exactas de una etiqueta (sin atributos) del
// elemento X: "<X>", "</X>" y ":X>" (con prefijo, de apertura o de cierre).
// Ninguna coincide con un elemento cuyo nombre termina igual: "CodX>" va
// precedido de "<", "</" o ":" + "CodX", nunca de "<X" ni de ":X".
func etiquetas(x string) []string {
	return []string{"<" + x + ">", "</" + x + ">", ":" + x + ">"}
}

// marcarEtiquetas devuelve b con cada etiqueta de X cambiada por
// marcaEtiqueta.
func marcarEtiquetas(b interface{}, x string) interface{} {
	for _, e := range etiquetas(x) {
		b = reemplazar(b, e, marcaEtiqueta)
	}
	return b
}

// reemplazar cambia cada aparicion de de por a, con $split y $reduce (sirve
// en cualquier version de Mongo, sin $replaceAll).
func reemplazar(b interface{}, de, a string) bson.D {
	return bson.D{{Key: "$reduce", Value: bson.D{
		{Key: "input", Value: bson.D{{Key: "$split", Value: bson.A{b, de}}}},
		{Key: "initialValue", Value: nil},
		{Key: "in", Value: cond(eq("$$value", nil), "$$this", bson.D{{Key: "$concat", Value: bson.A{"$$value", a, "$$this"}}})},
	}}}
}

// sinAtributos quita los atributos de cada etiqueta de apertura que empieza
// con inicio ("<X" o ":X"): "<X xmlns="...">" pasa a "<X>" y "<X a="1"/>"
// a "<X/>". Parte el texto por inicio + " " y, en cada trozo menos el
// primero, descarta hasta el primer ">". Si una etiqueta no cierra, el texto
// entero queda ilegible.
func sinAtributos(b, inicio string) bson.D {
	sep := inicio + " "
	return cond(eq(b, marcaMala), marcaMala, bson.D{{Key: "$let", Value: bson.D{
		{Key: "vars", Value: bson.D{{Key: "t", Value: bson.D{{Key: "$split", Value: bson.A{b, sep}}}}}},
		{Key: "in", Value: bson.D{{Key: "$reduce", Value: bson.D{
			{Key: "input", Value: bson.D{{Key: "$slice", Value: bson.A{"$$t", 1, bson.D{{Key: "$size", Value: "$$t"}}}}}},
			{Key: "initialValue", Value: bson.D{{Key: "$arrayElemAt", Value: bson.A{"$$t", 0}}}},
			{Key: "in", Value: cond(eq("$$value", marcaMala), marcaMala, bson.D{{Key: "$let", Value: bson.D{
				{Key: "vars", Value: bson.D{{Key: "k", Value: idx("$$this", ">")}}},
				{Key: "in", Value: cond(lt("$$k", 0), marcaMala, bson.D{{Key: "$concat", Value: bson.A{
					"$$value",
					inicio,
					cond(bson.D{{Key: "$and", Value: bson.A{
						gt("$$k", 0),
						eq(bson.D{{Key: "$substrCP", Value: bson.A{"$$this", bson.D{{Key: "$subtract", Value: bson.A{"$$k", 1}}}, 1}}}, "/"),
					}}}, "/>", ">"),
					bson.D{{Key: "$substrCP", Value: bson.A{"$$this", bson.D{{Key: "$add", Value: bson.A{"$$k", 1}}}, bson.D{{Key: "$strLenCP", Value: "$$this"}}}}},
				}}})},
			}}})},
		}}}},
	}}})
}

// quitarBloque elimina de b todos los bloques <X>...</X>, con o sin
// prefijo (b ya viene sin atributos en las etiquetas de X). Parte el texto
// por las etiquetas exactas de X: los trozos pares quedan fuera de los
// bloques y los impares dentro. Si la cantidad de trozos no cuadra, el
// texto entero queda ilegible.
func quitarBloque(b, x string) bson.D {
	return cond(eq(b, marcaMala), marcaMala,
		bson.D{{Key: "$let", Value: bson.D{
			{Key: "vars", Value: bson.D{{Key: "t", Value: bson.D{{Key: "$split", Value: bson.A{marcarEtiquetas(b, x), marcaEtiqueta}}}}}},
			{Key: "in", Value: cond(
				eq(bson.D{{Key: "$mod", Value: bson.A{bson.D{{Key: "$size", Value: "$$t"}}, 2}}}, 1),
				bson.D{{Key: "$reduce", Value: bson.D{
					{Key: "input", Value: bson.D{{Key: "$range", Value: bson.A{0, bson.D{{Key: "$size", Value: "$$t"}}, 2}}}},
					{Key: "initialValue", Value: ""},
					{Key: "in", Value: bson.D{{Key: "$concat", Value: bson.A{"$$value", bson.D{{Key: "$arrayElemAt", Value: bson.A{"$$t", "$$this"}}}}}}},
				}}},
				marcaMala,
			)},
		}}},
	)
}

// contar devuelve cuantas veces viene el elemento X en b (pares de
// etiquetas mas las vacias "<X/>"), o -1 si no se puede contar con
// seguridad.
func contar(b, x string) bson.D {
	partes := func(sep string) bson.D {
		return bson.D{{Key: "$subtract", Value: bson.A{bson.D{{Key: "$size", Value: bson.D{{Key: "$split", Value: bson.A{b, sep}}}}}, 1}}}
	}
	return cond(eq(b, marcaMala), -1,
		bson.D{{Key: "$let", Value: bson.D{
			// etiquetas de apertura y cierre: dos por elemento
			{Key: "vars", Value: bson.D{{Key: "n", Value: bson.D{{Key: "$add", Value: bson.A{
				partes(etiquetas(x)[0]), partes(etiquetas(x)[1]), partes(etiquetas(x)[2]),
			}}}}}},
			{Key: "in", Value: cond(eq(bson.D{{Key: "$mod", Value: bson.A{"$$n", 2}}}, 0),
				bson.D{{Key: "$add", Value: bson.A{
					bson.D{{Key: "$toInt", Value: bson.D{{Key: "$divide", Value: bson.A{"$$n", 2}}}}},
					partes("<" + x + "/>"),
					partes(":" + x + "/>"),
				}}},
				-1)},
		}}},
	)
}

func idx(b, s string) bson.D {
	return bson.D{{Key: "$indexOfCP", Value: bson.A{b, s}}}
}

func idxDesde(b, s string, desde interface{}) bson.D {
	return bson.D{{Key: "$indexOfCP", Value: bson.A{b, s, desde}}}
}

func cond(si, entonces, sino interface{}) bson.D {
	return bson.D{{Key: "$cond", Value: bson.A{si, entonces, sino}}}
}

func eq(a, b interface{}) bson.D  { return bson.D{{Key: "$eq", Value: bson.A{a, b}}} }
func lt(a, b interface{}) bson.D  { return bson.D{{Key: "$lt", Value: bson.A{a, b}}} }
func gt(a, b interface{}) bson.D  { return bson.D{{Key: "$gt", Value: bson.A{a, b}}} }
func gte(a, b interface{}) bson.D { return bson.D{{Key: "$gte", Value: bson.A{a, b}}} }
func trim(v interface{}) bson.D {
	return bson.D{{Key: "$trim", Value: bson.D{{Key: "input", Value: v}}}}
}

func contiene(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

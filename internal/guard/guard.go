// Package guard valida los pipelines de agregacion que arma el modelo antes
// de ejecutarlos. Parte del guard de Hermes AI (mcp-server) y agrega reglas
// propias de este servidor:
//
//  1. Lista blanca de etapas de solo lectura, en cualquier nivel.
//  2. Operadores prohibidos en cualquier posicion: los que escriben, los que
//     ejecutan JavaScript en el servidor ($where, $function, $accumulator)
//     y los que exponen su estado interno.
//  3. El "from" de $lookup/$graphLookup tiene que ser una coleccion
//     permitida (test_inputs, test_outputs).
//  4. Ningun objeto puede repetir una clave (ValidarClavesUnicas).
//  5. Las ramas de $facet y el "pipeline" de $lookup se validan igual.
//
// Es una lista blanca por nombre, no un analisis del costo: lo acota el
// tiempo maximo por consulta, que Mongo recibe como maxTimeMS.
package guard

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

var etapasPermitidas = map[string]bool{
	"$match":       true,
	"$project":     true,
	"$addFields":   true,
	"$set":         true,
	"$unset":       true,
	"$sort":        true,
	"$limit":       true,
	"$count":       true,
	"$group":       true,
	"$unwind":      true,
	"$lookup":      true,
	"$facet":       true,
	"$replaceRoot": true,
	"$replaceWith": true,
	"$graphLookup": true,
	"$sample":      true,
	"$bucket":      true,
	"$bucketAuto":  true,
	"$sortByCount": true,
}

var clavesProhibidas = map[string]bool{
	"$out":               true,
	"$merge":             true,
	"$where":             true,
	"$function":          true,
	"$accumulator":       true,
	"$currentOp":         true,
	"$indexStats":        true,
	"$collStats":         true,
	"$planCacheStats":    true,
	"$listSessions":      true,
	"$listLocalSessions": true,
	"$eval":              true,
}

// Guard valida pipelines contra las colecciones permitidas.
type Guard struct {
	colecciones map[string]bool
	texto       string
}

func New(colecciones []string) *Guard {
	g := &Guard{colecciones: map[string]bool{}}
	nombres := append([]string(nil), colecciones...)
	sort.Strings(nombres)
	for _, c := range nombres {
		g.colecciones[c] = true
	}
	g.texto = strings.Join(nombres, ", ")
	return g
}

// Opciones cambia reglas para un uso concreto.
type Opciones struct {
	// SinFacet rechaza $facet: analizar_mensajes ejecuta las etapas del
	// modelo dentro de un $facet propio, y Mongo no admite uno dentro de
	// otro.
	SinFacet bool
}

// Validar aplica las reglas a un pipeline en texto JSON. Devuelve las etapas
// tal como llegaron, que es lo que se ejecuta (decodificarlas a map
// perderia el orden de las claves de un $sort).
func (g *Guard) Validar(pipeline []json.RawMessage, op Opciones) error {
	if err := ValidarClavesUnicas(pipeline); err != nil {
		return err
	}
	etapas := make([]interface{}, len(pipeline))
	for i, raw := range pipeline {
		var m map[string]interface{}
		if err := json.Unmarshal(raw, &m); err != nil {
			return fmt.Errorf("etapa %d: no es un objeto JSON valido: %w", i, err)
		}
		etapas[i] = m
	}
	return g.validarEtapas(etapas, "", op)
}

func (g *Guard) validarEtapas(etapas []interface{}, ruta string, op Opciones) error {
	for i, e := range etapas {
		etapa, ok := e.(map[string]interface{})
		if !ok || len(etapa) != 1 {
			return fmt.Errorf("%setapa %d invalida: se esperaba un objeto con un unico operador de etapa, ej. {\"$match\": {...}}", ruta, i)
		}
		for nombre, cuerpo := range etapa {
			if !etapasPermitidas[nombre] {
				return fmt.Errorf("%setapa %d no permitida: %q -- solo etapas de lectura", ruta, i, nombre)
			}
			if nombre == "$facet" && op.SinFacet {
				return fmt.Errorf("%setapa %d: $facet no se puede usar aqui (las etapas ya corren dentro de un $facet)", ruta, i)
			}
			if nombre == "$lookup" || nombre == "$graphLookup" {
				if err := g.validarFrom(cuerpo); err != nil {
					return fmt.Errorf("%setapa %d (%s): %w", ruta, i, nombre, err)
				}
			}
			if err := g.validarSubPipelines(nombre, cuerpo, fmt.Sprintf("%setapa %d (%s) > ", ruta, i, nombre), op); err != nil {
				return err
			}
		}
		if err := revisar(etapa); err != nil {
			return fmt.Errorf("%setapa %d: %w", ruta, i, err)
		}
	}
	return nil
}

func (g *Guard) validarSubPipelines(nombre string, cuerpo interface{}, ruta string, op Opciones) error {
	switch nombre {
	case "$facet":
		ramas, ok := cuerpo.(map[string]interface{})
		if !ok {
			return fmt.Errorf("%sse esperaba un objeto con las ramas", ruta)
		}
		for rama, sub := range ramas {
			lista, ok := sub.([]interface{})
			if !ok {
				return fmt.Errorf("%s%s: se esperaba una lista de etapas", ruta, rama)
			}
			if err := g.validarEtapas(lista, ruta+rama+" > ", op); err != nil {
				return err
			}
		}
	case "$lookup":
		b, ok := cuerpo.(map[string]interface{})
		if !ok {
			return nil
		}
		if sub, ok := b["pipeline"]; ok {
			lista, ok := sub.([]interface{})
			if !ok {
				return fmt.Errorf("%spipeline: se esperaba una lista de etapas", ruta)
			}
			return g.validarEtapas(lista, ruta+"pipeline > ", op)
		}
	}
	return nil
}

func (g *Guard) validarFrom(cuerpo interface{}) error {
	b, ok := cuerpo.(map[string]interface{})
	if !ok {
		return nil
	}
	from, ok := b["from"]
	if !ok {
		return nil // Mongo lo rechaza al ejecutar
	}
	s, ok := from.(string)
	if !ok || !g.colecciones[s] {
		return fmt.Errorf("\"from\": %v no es una coleccion permitida (%s)", from, g.texto)
	}
	return nil
}

// revisar recorre cualquier valor buscando claves prohibidas.
func revisar(v interface{}) error {
	switch val := v.(type) {
	case map[string]interface{}:
		for k, inner := range val {
			if clavesProhibidas[k] {
				return fmt.Errorf("operador no permitido: %q", k)
			}
			if err := revisar(inner); err != nil {
				return err
			}
		}
	case []interface{}:
		for _, inner := range val {
			if err := revisar(inner); err != nil {
				return err
			}
		}
	}
	return nil
}

// ValidarClavesUnicas rechaza un pipeline si algun objeto repite una clave:
// la validacion trabaja sobre el JSON decodificado, donde una clave repetida
// queda con su ultimo valor, pero Mongo ejecuta el texto original con las
// dos.
func ValidarClavesUnicas(pipeline []json.RawMessage) error {
	for i, raw := range pipeline {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := leerSinRepetidas(dec); err != nil {
			return fmt.Errorf("etapa %d: %w", i, err)
		}
	}
	return nil
}

func leerSinRepetidas(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("no es JSON valido: %w", err)
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		vistas := map[string]bool{}
		for dec.More() {
			t, err := dec.Token()
			if err != nil {
				return fmt.Errorf("no es JSON valido: %w", err)
			}
			clave, _ := t.(string)
			if vistas[clave] {
				return fmt.Errorf("la clave %q esta repetida en el mismo objeto", clave)
			}
			vistas[clave] = true
			if err := leerSinRepetidas(dec); err != nil {
				return err
			}
		}
	case '[':
		for dec.More() {
			if err := leerSinRepetidas(dec); err != nil {
				return err
			}
		}
	}
	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("no es JSON valido: %w", err)
	}
	return nil
}

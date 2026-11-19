// Package conocimiento carga el fichero de conocimiento (conocimiento/
// flows.json): por cada flow, el formato del XML de entrada (campos, tipos,
// obligatorios), tal como lo documentan las fichas del flow de Mule.
package conocimiento

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Forma de un campo del XML.
const (
	FormaValor = "valor" // elemento simple: se extrae su texto
	FormaLista = "lista" // elemento complejo o repetible: se cuenta cuantas veces viene
)

type Campo struct {
	Nombre      string   `json:"nombre"`
	Tipo        string   `json:"tipo"`
	Obligatorio bool     `json:"obligatorio"`
	Forma       string   `json:"forma"`
	Valores     []string `json:"valores,omitempty"`
	Notas       string   `json:"notas,omitempty"`
	// Hijos son los nombres de todos los elementos que van dentro de un
	// campo de forma lista, a cualquier profundidad. Sirven para saber que
	// bloques hay que quitar antes de buscar un campo de primer nivel con
	// el mismo nombre (por ejemplo, Nombre dentro de ListadoDireccion).
	Hijos []string `json:"hijos,omitempty"`
}

type Flow struct {
	Flow         string  `json:"flow"`
	NombreComun  string  `json:"nombreComun"`
	ElementoRaiz string  `json:"elementoRaiz"`
	Campos       []Campo `json:"campos"`
}

type Conocimiento struct {
	Descripcion string `json:"descripcion"`
	Flows       []Flow `json:"flows"`
}

// Cargar lee y valida el fichero.
func Cargar(ruta string) (*Conocimiento, error) {
	b, err := os.ReadFile(ruta)
	if err != nil {
		return nil, fmt.Errorf("no se pudo leer %s: %w", ruta, err)
	}
	var c Conocimiento
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("%s no es valido: %w", ruta, err)
	}
	if err := c.validar(); err != nil {
		return nil, fmt.Errorf("%s: %w", ruta, err)
	}
	return &c, nil
}

func (c *Conocimiento) validar() error {
	if len(c.Flows) == 0 {
		return fmt.Errorf("no hay flows")
	}
	vistos := map[string]bool{}
	for _, f := range c.Flows {
		if f.Flow == "" || f.ElementoRaiz == "" {
			return fmt.Errorf("flow sin nombre o sin elementoRaiz")
		}
		if vistos[f.Flow] {
			return fmt.Errorf("flow repetido: %s", f.Flow)
		}
		vistos[f.Flow] = true
		if err := f.validar(); err != nil {
			return fmt.Errorf("flow %s: %w", f.Flow, err)
		}
	}
	return nil
}

func (f Flow) validar() error {
	campos := map[string]bool{}
	for _, c := range f.Campos {
		if c.Nombre == "" || strings.ContainsAny(c.Nombre, "<>:/ ") {
			return fmt.Errorf("nombre de campo invalido: %q", c.Nombre)
		}
		if campos[c.Nombre] {
			return fmt.Errorf("campo repetido: %s", c.Nombre)
		}
		campos[c.Nombre] = true
		switch c.Forma {
		case FormaValor:
			if len(c.Hijos) > 0 {
				return fmt.Errorf("%s: un campo de forma valor no tiene hijos", c.Nombre)
			}
		case FormaLista:
		default:
			return fmt.Errorf("%s: forma %q invalida (valor o lista)", c.Nombre, c.Forma)
		}
	}
	return nil
}

// Flow devuelve el flow por nombre.
func (c *Conocimiento) Flow(nombre string) (Flow, bool) {
	for _, f := range c.Flows {
		if f.Flow == nombre {
			return f, true
		}
	}
	return Flow{}, false
}

// NombresFlows lista los flows en el orden del fichero.
func (c *Conocimiento) NombresFlows() []string {
	out := make([]string, len(c.Flows))
	for i, f := range c.Flows {
		out[i] = f.Flow
	}
	return out
}

// Campo devuelve un campo del flow por nombre.
func (f Flow) Campo(nombre string) (Campo, bool) {
	for _, c := range f.Campos {
		if c.Nombre == nombre {
			return c, true
		}
	}
	return Campo{}, false
}

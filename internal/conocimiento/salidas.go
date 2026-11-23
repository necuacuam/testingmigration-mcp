package conocimiento

// Salidas: el fichero conocimiento/salidas.json. Por cada flow de entrada,
// los mensajes que Mule envia a cada destino (lo que queda en message_body
// de test_outputs), tomados del DataWeave del sub-flow de backend que los
// arma y los destinos de la configuracion de produccion de cada repo.

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Formas de un campo de salida y formatos de mensaje.
const (
	FormaObjeto = "objeto" // elemento con hijos que viene una vez
	FormatoXML  = "xml"
	FormatoJSON = "json"
)

type CampoSalida struct {
	Nombre string `json:"nombre"`
	Forma  string `json:"forma"`
	// Origen es la ruta del campo de la entrada que usa (texto) o varias
	// (lista), tal como sale del DataWeave.
	Origen            json.RawMessage `json:"origen,omitempty"`
	Expresion         string          `json:"expresion,omitempty"`
	Variables         []string        `json:"variables,omitempty"`
	Condicion         string          `json:"condicion,omitempty"`
	Nota              string          `json:"nota,omitempty"`
	NoEstaEnLaEntrada bool            `json:"noEstaEnLaEntrada,omitempty"`
	Campos            []CampoSalida   `json:"campos,omitempty"`
}

type DestinoSalida struct {
	Destino         string   `json:"destino"`
	Configuraciones []string `json:"configuraciones"`
}

type Salida struct {
	Subflow      string            `json:"subflow"`
	Repo         string            `json:"repo"`
	Fichero      string            `json:"fichero"`
	Formato      string            `json:"formato"`
	Tipo         string            `json:"tipo,omitempty"`
	Destinos     []DestinoSalida   `json:"destinos"`
	Contenedores string            `json:"contenedores,omitempty"`
	ElementoRaiz string            `json:"elementoRaiz,omitempty"`
	Namespaces   map[string]string `json:"namespaces,omitempty"`
	Entrada      map[string]string `json:"entrada,omitempty"`
	Campos       []CampoSalida     `json:"campos"`
	Nota         string            `json:"nota,omitempty"`
}

type FlowSalidas struct {
	Flow    string   `json:"flow"`
	Salidas []Salida `json:"salidas"`
}

type Salidas struct {
	Descripcion string        `json:"descripcion"`
	Flows       []FlowSalidas `json:"flows"`
}

// CargarSalidas lee y valida el fichero.
func CargarSalidas(ruta string) (*Salidas, error) {
	b, err := os.ReadFile(ruta)
	if err != nil {
		return nil, fmt.Errorf("no se pudo leer %s: %w", ruta, err)
	}
	var s Salidas
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("%s no es valido: %w", ruta, err)
	}
	if err := s.validar(); err != nil {
		return nil, fmt.Errorf("%s: %w", ruta, err)
	}
	return &s, nil
}

func (s *Salidas) validar() error {
	if len(s.Flows) == 0 {
		return fmt.Errorf("no hay flows")
	}
	vistos := map[string]bool{}
	for _, f := range s.Flows {
		if f.Flow == "" || vistos[f.Flow] {
			return fmt.Errorf("flow vacio o repetido: %q", f.Flow)
		}
		vistos[f.Flow] = true
		destinos := map[string]string{}
		for _, x := range f.Salidas {
			if x.Formato != FormatoXML && x.Formato != FormatoJSON {
				return fmt.Errorf("%s %s: formato %q invalido (xml o json)", f.Flow, x.Subflow, x.Formato)
			}
			for _, d := range x.Destinos {
				if otro, ya := destinos[d.Destino]; ya {
					return fmt.Errorf("%s: el destino %s esta en %s y en %s", f.Flow, d.Destino, otro, x.Subflow)
				}
				destinos[d.Destino] = x.Subflow
			}
			if err := validarCamposSalida(x.Campos); err != nil {
				return fmt.Errorf("%s %s: %w", f.Flow, x.Subflow, err)
			}
		}
	}
	return nil
}

func validarCamposSalida(cs []CampoSalida) error {
	vistos := map[string]CampoSalida{}
	for _, c := range cs {
		if c.Nombre == "" || strings.ContainsAny(c.Nombre, "<>:/.$ \"") {
			return fmt.Errorf("nombre de campo invalido: %q", c.Nombre)
		}
		// Un DataWeave puede escribir la misma clave dos veces (el mensaje
		// la lleva dos veces); se acepta si las dos son iguales: se lee la
		// primera y es el mismo valor.
		if previo, ya := vistos[c.Nombre]; ya {
			a, _ := json.Marshal(previo)
			b, _ := json.Marshal(c)
			if string(a) != string(b) {
				return fmt.Errorf("campo repetido con distinta definicion: %s", c.Nombre)
			}
			continue
		}
		vistos[c.Nombre] = c
		switch c.Forma {
		case FormaValor:
			if len(c.Campos) > 0 {
				return fmt.Errorf("%s: un campo de forma valor no tiene campos", c.Nombre)
			}
		case FormaLista, FormaObjeto:
		default:
			return fmt.Errorf("%s: forma %q invalida (valor, lista u objeto)", c.Nombre, c.Forma)
		}
		if err := validarCamposSalida(c.Campos); err != nil {
			return fmt.Errorf("%s: %w", c.Nombre, err)
		}
	}
	return nil
}

// Flow devuelve las salidas de un flow.
func (s *Salidas) Flow(nombre string) (FlowSalidas, bool) {
	for _, f := range s.Flows {
		if f.Flow == nombre {
			return f, true
		}
	}
	return FlowSalidas{}, false
}

// NombresFlows lista los flows con salidas, en el orden del fichero.
func (s *Salidas) NombresFlows() []string {
	out := make([]string, len(s.Flows))
	for i, f := range s.Flows {
		out[i] = f.Flow
	}
	return out
}

// PorDestino devuelve la salida del flow que va al destino, tal como se
// guarda en test_outputs.destino.
func (f FlowSalidas) PorDestino(destino string) (Salida, bool) {
	for _, x := range f.Salidas {
		for _, d := range x.Destinos {
			if d.Destino == destino {
				return x, true
			}
		}
	}
	return Salida{}, false
}

// Destinos lista los destinos conocidos del flow.
func (f FlowSalidas) Destinos() []string {
	var out []string
	for _, x := range f.Salidas {
		for _, d := range x.Destinos {
			out = append(out, d.Destino)
		}
	}
	return out
}

// Hijos devuelve los nombres de todos los campos que van dentro de c, a
// cualquier profundidad.
func (c CampoSalida) Hijos() []string {
	var out []string
	for _, h := range c.Campos {
		out = append(out, h.Nombre)
		out = append(out, h.Hijos()...)
	}
	return out
}

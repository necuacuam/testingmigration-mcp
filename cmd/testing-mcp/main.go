// Comando testing-mcp: servidor MCP de solo lectura sobre la base de pruebas
// de la migracion de Mule. Se comunica por stdio (Claude Desktop).
//
// Configuracion: un ".env" al lado del ejecutable o en el directorio de
// trabajo (ver .env.example). Una variable del entorno real gana sobre el
// .env.
package main

import (
	"bufio"
	"context"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // zonas horarias embebidas: Windows no las trae

	"github.com/mark3labs/mcp-go/server"

	"testing-mcp/internal/conocimiento"
	"testing-mcp/internal/store"
	"testing-mcp/internal/tools"
)

func main() {
	// stdout es exclusivo del protocolo MCP: los logs van a stderr.
	logger := log.New(os.Stderr, "[testing-mcp] ", log.LstdFlags)
	cargarDotEnv(logger)

	uri := os.Getenv("TESTING_MCP_MONGO_URI")
	if uri == "" {
		logger.Fatalf("TESTING_MCP_MONGO_URI no esta definida: completala en el .env (ver .env.example)")
	}
	base := os.Getenv("TESTING_MCP_MONGO_DB")
	if base == "" {
		base = "testing_migracion"
	}
	rutaConoc := os.Getenv("TESTING_MCP_CONOCIMIENTO")
	if rutaConoc == "" {
		rutaConoc = "conocimiento/flows.json"
	}
	conoc, err := conocimiento.Cargar(resolver(rutaConoc))
	if err != nil {
		logger.Fatalf("fichero de conocimiento: %v", err)
	}
	rutaSalidas := os.Getenv("TESTING_MCP_SALIDAS")
	if rutaSalidas == "" {
		rutaSalidas = "conocimiento/salidas.json"
	}
	salidas, err := conocimiento.CargarSalidas(resolver(rutaSalidas))
	if err != nil {
		logger.Fatalf("fichero de salidas: %v", err)
	}

	limite := 20
	if raw := os.Getenv("TESTING_MCP_LIMITE_PANTALLA"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			limite = v
		} else {
			logger.Printf("TESTING_MCP_LIMITE_PANTALLA=%q invalida, uso %d", raw, limite)
		}
	}
	timeout := 120 * time.Second
	if raw := os.Getenv("TESTING_MCP_MONGO_TIMEOUT"); raw != "" {
		if v, err := time.ParseDuration(raw); err == nil && v > 0 {
			timeout = v
		} else {
			logger.Printf("TESTING_MCP_MONGO_TIMEOUT=%q invalida (formato 90s, 2m), uso %s", raw, timeout)
		}
	}

	zona := time.UTC
	nombreZona := os.Getenv("TESTING_MCP_ZONA_HORARIA")
	if nombreZona == "" {
		nombreZona = "Europe/Madrid"
	}
	if z, err := time.LoadLocation(nombreZona); err == nil {
		zona = z
	} else {
		logger.Printf("TESTING_MCP_ZONA_HORARIA=%q invalida, uso UTC", nombreZona)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	st, err := store.New(ctx, uri, base)
	cancel()
	if err != nil {
		logger.Fatalf("%v", err)
	}
	logger.Printf("conectado a %s (base %s); %d flows; %d con salidas; limite %d filas; tiempo maximo %s",
		store.URISinClave(uri), base, len(conoc.Flows), len(salidas.Flows), limite, timeout)

	s := server.NewMCPServer("testing-mcp", "0.1.0",
		server.WithToolCapabilities(false),
		server.WithInstructions(tools.Instrucciones),
	)
	tools.Registrar(s, tools.Config{
		Conocimiento:   conoc,
		Salidas:        salidas,
		Ejecutor:       st,
		LimitePantalla: limite,
		Timeout:        timeout,
		Zona:           zona,
	})
	if err := server.ServeStdio(s, server.WithErrorLogger(logger)); err != nil {
		logger.Fatalf("error fatal: %v", err)
	}
}

// resolver busca una ruta relativa en el directorio de trabajo y despues al
// lado del ejecutable: Claude Desktop lo lanza con un directorio de trabajo
// que no se controla.
func resolver(nombre string) string {
	if filepath.IsAbs(nombre) {
		return nombre
	}
	if cwd, err := os.Getwd(); err == nil {
		if c := filepath.Join(cwd, nombre); existe(c) {
			return c
		}
	}
	if exe, err := os.Executable(); err == nil {
		if c := filepath.Join(filepath.Dir(exe), nombre); existe(c) {
			return c
		}
	}
	return nombre
}

func existe(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// cargarDotEnv completa las variables que falten con el ".env" del
// directorio de trabajo y el del ejecutable. Formato clave=valor, sin
// comillas; "#" al inicio de linea es comentario.
func cargarDotEnv(logger *log.Logger) {
	var rutas []string
	if cwd, err := os.Getwd(); err == nil {
		rutas = append(rutas, filepath.Join(cwd, ".env"))
	}
	if exe, err := os.Executable(); err == nil {
		rutas = append(rutas, filepath.Join(filepath.Dir(exe), ".env"))
	}
	for _, r := range rutas {
		f, err := os.Open(r)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			l := strings.TrimSpace(sc.Text())
			if l == "" || strings.HasPrefix(l, "#") {
				continue
			}
			k, v, ok := strings.Cut(l, "=")
			k = strings.TrimSpace(k)
			if !ok || k == "" {
				continue
			}
			if _, ya := os.LookupEnv(k); !ya {
				os.Setenv(k, strings.TrimSpace(v))
			}
		}
		if err := sc.Err(); err != nil {
			logger.Printf("aviso: error leyendo %s: %v", r, err)
		}
		f.Close()
	}
}

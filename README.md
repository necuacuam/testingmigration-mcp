# testing-mcp

Servidor MCP de solo lectura para estudiar el contenido de los mensajes de entrada de los flows de Mule: el XML guardado en `message_body` de `test_inputs`. Permite preguntar en lenguaje natural, por ejemplo: "¿qué porcentaje de mensajes de GrupoCliente tiene `Codigo` no numérico y `CodigoNormalizado` con valor?".

Parte del MCP de Hermes AI (`mcp-server`) y usa el mismo guard de pipelines.

## Flows cubiertos

56 flows, todos los que tienen ficha de entrada:

| Área | Flows |
|---|---|
| Comercial (17) | `GrupoCliente`, `Cliente`, `Objetivo` (centros), `PuntoServicio`, `CalendarioClienteObjetivo`, `CatalogoServicios`, `Empresa`, `EstructuraProsegur`, `MaestroCondicionPago`, `MaestroDivisiones`, `MaestroEscalas`, `MaestroSector`, `MaestroServicios`, `MaestroSubescalas`, `ObjetivoSubtercero`, `OrdenTrabajo`, `PeriodoCierreClientes` |
| RRHH (26) | `AbsentismoDeEmpleados`, `AsientosDeSueldos`, `CalendarioManoObra`, `CargarHorasCompensar`, `CategoriasSalariales`, `ConsultaAbsentismosEmpleados`, `ConsultaFicherosMO`, `CosteEmpleados`, `DatosLiquidacion`, `Empleados`, `EnvioFiniquitos`, `EstructuraGeografica`, `FechasFiniquitos`, `FicheroFiniquitos`, `FormacionEmpleado`, `InformacionCierresFiniquitos`, `MaestroAbsentismos`, `MaestroBeneficios`, `MaestroCategorias`, `MaestroConceptos`, `MaestroLugaresPago`, `MaestroMotivosFiniquito`, `MaestroTiposContrato`, `ProcesarContratoEmpleado`, `ProcesarEmpleado`, `RetrocesoFiniquitos` |
| Selfor (4) | `HabilitacionesEmpleados`, `MaestroAccionesFormativas`, `MaestroNivelesFormacion`, `MaestroTiposHabilitaciones` |
| ERP (5) | `CostesMateriales`, `HorasReclasificacionGastos`, `MaestroTiposMateriales`, `MaterialesAFecha`, `MovimientosMateriales` |
| Motor (2) | `ConsultaMotor`, `IntegracionWFMMotor` |
| Opera (1) | `ConsultaCuadranteParametrizado` |
| VCM (1) | `ServicioVCM` |

En `test_inputs`, cada uno se llama con el sufijo `-flow` (por ejemplo, `GrupoCliente-flow`).

El formato del mensaje de cada flow está en `conocimiento/flows.json`, tal como lo documentan las fichas hechas a partir del flow de Mule. Para sumar un flow, se agrega ahí: no hace falta tocar el código. Ese fichero no está en el repositorio: se copia aparte.

Los campos que van dentro de una lista (por ejemplo, `CodEmpresa` dentro de `Empresa`) no se leen uno por uno: de la lista se cuenta cuántas veces viene.

## Herramientas

| Herramienta | Qué hace |
|---|---|
| `diccionario` | Formato del mensaje de entrada de un flow: elemento raíz y campos con tipo, obligatorio, forma (valor o lista), valores posibles y notas. No consulta la base. |
| `analizar_mensajes` | Lee en el servidor los campos pedidos del XML de los mensajes de un flow y ejecuta encima las etapas que arma el modelo. |

**Qué devuelve `analizar_mensajes`:**
- **`mensajes`:** cuántos mensajes se analizaron, en total y separados en reales y mock.
- **`lectura`:** por campo, cuántos mensajes lo traen con valor, vacío, ausente o ilegible.
- **`filas`:** el resultado de las etapas que armó el modelo.

**Qué recibe cada mensaje en esas etapas:**
- `correlation_id`
- `mock` (`true`/`false`)
- `event_timestamp`: cuándo Mule recibió el mensaje (UTC)
- `message_body`: el XML completo, para poder mostrarlo
- `presencia.<campo>`
- `campos.<campo>`: el texto tal cual o, en las listas, cuántas veces viene

**Mock:** el parámetro `origen` elige `todos` (por defecto), `reales` o `mock`.

**Fechas:** `desde` y `hasta` limitan el periodo antes de leer el XML, así `mensajes` y `lectura` cuentan solo ese periodo. Aceptan una fecha (`2026-10-01`, interpretada en `TESTING_MCP_ZONA_HORARIA`, por defecto Europe/Madrid; `hasta` incluye el día completo) o una fecha y hora con zona (`2026-10-01T08:00:00+02:00`). Para agrupar por día, mes u hora se usa `event_timestamp` en el pipeline (`$dateToString` o `$dateTrunc`).

## Reglas

- **Sin JavaScript y solo lectura.** El guard rechaza `$where`, `$function` y `$accumulator`, además de toda etapa que escriba (`$out`, `$merge`). Solo consulta `test_inputs`.
- **Lectura del XML con texto fijo** (`$indexOfCP`, `$substrCP`, `$split`), no con regex: una regex sobre `message_body` devolvió ceros sin dar error. La lectura:
  - acepta el campo con o sin prefijo de namespace, en cualquier orden;
  - lee el contenido de un CDATA;
  - trata `<Campo/>` como campo vacío;
  - no confunde un campo de primer nivel con uno del mismo nombre dentro de una lista (por ejemplo, `Nombre` dentro de `ListadoDireccion`);
  - no confunde una lista con otro elemento cuyo nombre termina igual (por ejemplo, `Division` con `CodDivision` o `TipoDivision`).

  - lee también los elementos con atributos, por ejemplo los mensajes que repiten `xmlns` en cada elemento (`<Codigo xmlns="...">0100200</Codigo>`).

  Lo que no puede leer con seguridad (una etiqueta o un CDATA sin cerrar, o una estructura que no cuadra) lo cuenta como ilegible y lo avisa.
- **Límites.** Devuelve como máximo `TESTING_MCP_LIMITE_PANTALLA` filas, con aviso si hay más. Cada consulta tiene un tiempo máximo, `TESTING_MCP_MONGO_TIMEOUT`, que Mongo también recibe como `maxTimeMS`.

## Instalación

1. Compilar (cerrar antes Claude Desktop, que deja el ejecutable bloqueado):

   ```bash
   go build -o testing-mcp.exe ./cmd/testing-mcp
   ```

2. Completar `TESTING_MCP_MONGO_URI` en el `.env` que está al lado del ejecutable (ver `.env.example`).
3. En Claude Desktop, **Settings → Developer → Edit Config**, agregar:

   ```json
   {
     "mcpServers": {
       "testing-mcp": {
         "command": "D:\\sharepoint\\mulesoft\\testing-mcp\\testing-mcp.exe"
       }
     }
   }
   ```

4. Reiniciar Claude Desktop por completo, cerrando el proceso y no solo la ventana. Hay que hacerlo también después de recompilar o de cambiar el `.env` o `flows.json`.

Para llevarlo a otra máquina hacen falta tres cosas en una misma carpeta: `testing-mcp.exe`, `.env` y `conocimiento/flows.json`.

## Pruebas

```bash
go test ./...
```

Necesitan `conocimiento/flows.json`.

Las de `internal/tools` corren contra un Mongo real si está definida `TESTING_MCP_PRUEBAS_URI`; si no, se saltan. Crean una base temporal `testing_mcp_pruebas` y la borran al terminar.

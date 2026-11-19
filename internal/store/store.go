// Package store ejecuta pipelines de agregacion de solo lectura contra la
// base de pruebas.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var claveEnURI = regexp.MustCompile(`(://[^:/@]*:)[^@/]*@`)

// URISinClave tapa la contrasena de una URI para los logs.
func URISinClave(uri string) string {
	return claveEnURI.ReplaceAllString(uri, "${1}****@")
}

type Store struct {
	db *mongo.Database
}

func New(ctx context.Context, uri, base string) (*Store, error) {
	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("no se pudo conectar a Mongo (%s): %w", URISinClave(uri), err)
	}
	if err := cli.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("Mongo no responde en %s: %w", URISinClave(uri), err)
	}
	return &Store{db: cli.Database(base)}, nil
}

// Ejecutar corre el pipeline y devuelve los documentos en JSON relajado
// (fechas como texto ISO).
func (s *Store) Ejecutar(ctx context.Context, coleccion string, pipeline []bson.D, timeout time.Duration) ([]map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	opts := options.Aggregate().SetMaxTime(timeout).SetBatchSize(500)
	cur, err := s.db.Collection(coleccion).Aggregate(ctx, pipeline, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var out []map[string]interface{}
	for cur.Next(ctx) {
		var d bson.D
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		b, err := bson.MarshalExtJSON(d, false, false)
		if err != nil {
			return nil, err
		}
		var m map[string]interface{}
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, cur.Err()
}

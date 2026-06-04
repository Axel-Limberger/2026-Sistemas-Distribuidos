package sensor

import (
	"math"
	"math/rand"
	"sync"
)

// Simulador genera lecturas de temperatura y guarda la última medición registrada.
type Simulador struct {
	mu            sync.RWMutex
	ultimaLectura float64
}

// NuevoSimulador crea un sensor simulado con una temperatura inicial para ser realista.
func NuevoSimulador() *Simulador {
	return &Simulador{
		ultimaLectura: 22.0 + rand.Float64()*5.0,
	}
}

// Leer generaá una nueva temperatura variando levemente respecto de la lectura anterior.
func (s *Simulador) Leer() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	variacion := rand.Float64()*1.0 - 0.5
	nuevaLectura := s.ultimaLectura + variacion

	if nuevaLectura < 15.0 {
		nuevaLectura = 15.0
	}

	if nuevaLectura > 35.0 {
		nuevaLectura = 35.0
	}

	s.ultimaLectura = math.Round(nuevaLectura*10) / 10

	return s.ultimaLectura
}

// ObtenerUltima devuelve la última temperatura registrada sin generar una nueva lectura.
func (s *Simulador) ObtenerUltima() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.ultimaLectura
}

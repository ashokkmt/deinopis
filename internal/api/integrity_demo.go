package api

import (
	"errors"
	"net/http"

	"deinopis/internal/simulator"
)

func (s *Server) handleIntegrityDemoStart(w http.ResponseWriter, r *http.Request) {
	if s.integrityDemo == nil {
		s.jsonError(w, http.StatusServiceUnavailable, "simulated integrity workflow is unavailable")
		return
	}
	summary, err := s.integrityDemo.Start(r.Context())
	if err != nil {
		s.jsonError(w, http.StatusServiceUnavailable, "could not start isolated synthetic run: "+err.Error())
		return
	}
	s.jsonResponse(w, http.StatusCreated, summary)
}

func (s *Server) handleIntegrityDemoVerify(w http.ResponseWriter, r *http.Request) {
	if s.integrityDemo == nil {
		s.jsonError(w, http.StatusServiceUnavailable, "simulated integrity workflow is unavailable")
		return
	}
	result, err := s.integrityDemo.Verify(r.Context(), r.PathValue("run"))
	if errors.Is(err, simulator.ErrRunNotFound) {
		s.jsonError(w, http.StatusNotFound, err.Error())
		return
	}
	if err != nil {
		s.jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.jsonResponse(w, http.StatusOK, result)
}

func (s *Server) handleIntegrityDemoTamper(w http.ResponseWriter, r *http.Request) {
	if s.integrityDemo == nil {
		s.jsonError(w, http.StatusServiceUnavailable, "simulated integrity workflow is unavailable")
		return
	}
	if err := s.integrityDemo.TamperDelete(r.Context(), r.PathValue("run")); err != nil {
		if errors.Is(err, simulator.ErrRunNotFound) {
			s.jsonError(w, http.StatusNotFound, err.Error())
			return
		}
		s.jsonError(w, http.StatusConflict, err.Error())
		return
	}
	s.jsonResponse(w, http.StatusOK, map[string]string{"mode": "SIMULATED", "status": "one synthetic raw parent deleted", "scope": "disposable demo collector only"})
}

func (s *Server) handleIntegrityDemoTamperTail(w http.ResponseWriter, r *http.Request) {
	if s.integrityDemo == nil {
		s.jsonError(w, http.StatusServiceUnavailable, "simulated integrity workflow is unavailable")
		return
	}
	if err := s.integrityDemo.TamperDeleteTail(r.Context(), r.PathValue("run")); err != nil {
		if errors.Is(err, simulator.ErrRunNotFound) {
			s.jsonError(w, http.StatusNotFound, err.Error())
			return
		}
		s.jsonError(w, http.StatusConflict, err.Error())
		return
	}
	s.jsonResponse(w, http.StatusOK, map[string]string{"mode": "SIMULATED", "status": "synthetic tail batch deleted", "scope": "disposable demo collector only"})
}

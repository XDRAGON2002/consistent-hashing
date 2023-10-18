package consistenthash

import "errors"

// Struct of a server
type Server struct {
	id   string
	data map[string]string
}

// Initialize a new server
func getNewServer(id string) *Server {
	return &Server{
		id: id,
		data: make(map[string]string),
	}
}

// Method to get the value of a given key from the server
func (ss *Server) get(key string) (string, error) {
	if val, ok := ss.data[key]; ok {
		return val, nil
	}
	return "", errors.New("Key not found")
}

// Method to store the value of a given key value pair on the server
func (ss *Server) put(key, value string) error {
	ss.data[key] = value
	return nil
}

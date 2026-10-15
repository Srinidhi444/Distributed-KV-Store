package store

type Store struct {
	data map[string]string
}

func NewStore() *Store {
	myMap := make(map[string]string)
	return &Store{
		data: myMap,
	}
}

func (s *Store) Set(key string, value string) {
	if key == "" {
		return
	}
	s.data[key] = value
}

func (s *Store) Get(key string) (string, bool) {
	value, exists := s.data[key]
	return value, exists
}

func (s *Store) Delete(key string) {
	delete(s.data, key)
}

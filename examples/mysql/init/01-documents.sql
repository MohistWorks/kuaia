CREATE TABLE documents (
  id BIGINT PRIMARY KEY,
  content VARCHAR(255) NOT NULL
);

INSERT INTO documents (id, content) VALUES
  (1, 'Alpha document for Kuaia MySQL import'),
  (2, 'Beta document for local vector preparation'),
  (3, 'Gamma document for connector validation');

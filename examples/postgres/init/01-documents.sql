create table if not exists documents (
  id bigint primary key,
  content text not null
);

insert into documents (id, content) values
  (1, 'Alpha'),
  (2, 'Beta')
on conflict (id) do update set content = excluded.content;

create extension if not exists vector;

create table if not exists document_vectors (
  id bigint primary key,
  content text not null,
  embedding vector(4) not null
);

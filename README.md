# poc-multi-queues-celery-auto-scale

Prova de conceito para criar um mecanismo de auto scale com filas dinâmicas para o uso do Celery.

O mecanismo de escala implementado analisa.
* A proporção entre numero máximo de workers versus a quantidade de workers ativos.
* Se há a necessidade de criação de novos workers com base na quantidade de tarefas pendentes.

É uma heurística básica que pode ser aprimorada!

## Requisitos
* Python 3.11
* Mongo
* Redis
* Celery


## Uso

1. Suba o docker-compose com as dependências do Mongo e Redis. O Redis será utilizado com Backend e 
Broker para o Celery.
2. Crie uma coleção chamada `workers` na base do Mongo que armazenará os registros de workers ativos.
Essa coleção deverá ter um índice com expiração por TTL na coluna `last_ping_at`.
3. Rode o comando abaixo para criar as filas necessárias para o teste.
```shell
python -m app.repository
```
4. Em outros terminais execute os comandos abaixo:
* Gerar tarefas do tipo 1 que serão processadas pela fila default
```shell
python -m app.test
```
* Gerar tarefas do tipo 2 que serão processadas pela fila fila2
```shell
python -m app.test2
```
* Manter fallback de contagem de workers em dia.
```shell
python -m app.worker refresh
```
5. Para começar a escalar o número workers faça (em um novo terminal):
```shell
pyhton -m app.worker
```

O último comando irá analisar se há tarefas suficientes para a criação de um novo worker. Caso precise,
o worker será iniciado com base no tipo de fila necessária. 
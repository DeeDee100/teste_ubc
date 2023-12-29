link do teste: https://github.com/joaomarcelo81/DesafioUBC

docker run -d -p 8983:8983 --name solr_instance -t solr

docker exec -it solr_instance solr create_core -c gettingstarted

http://localhost:8983/solr/admin/cores?action=CREATE&name=gettingstarted&instanceDir=gettingstarted

create core -> requests.post('http://localhost:8983/solr/admin/cores?action=CREATE&name=gettingstarted&instanceDir=gettingstarted')
<!-- tambem funciona com requests.get -->
#Создаем новый том
docker volume create --name postgres_dwh_netology

#Проверяем
docker volume ls

#Запускаем контейнер
docker run --rm --name pg-docker -e POSTGRES_PASSWORD=***** -d -p 5400:5432 -v postgres_dwh_netology:/var/lib/postgresql/data postgres

#Проверяем
docker container ls

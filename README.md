# Задача
Задача: написать супер-минималистичный краулер

Мы запускаем скрипт, даём ему начальный URL, говорим в сколько потоков нужно краулить.

После того, как мы остановили скрипт, мы можем из него получить список адресов, которые он посетил (возможно, для этого нужно запустить какой-то скрипт, который нам эти адреса выдаст).

Содержимое страниц сохранять не нужно. Для извлечения адресов из страниц можно использовать любые средства, хоть регулярки (конечно, более уместные средства приветствуются).

Потенциально мы можем захотеть запускать потоки краулера на другой машине (не нужно это реализовывать, но нужно описать, что для этого осталось доделать в решении).

В решении нас в первую очередь интересует минимальная работоспособность, устройство проекта и культура кода.


# Решение

 
Пока успел реализовать лишь минимальную работоспособность с плохим качеством 
кода. 
Я буду считать что защиты от парсинга нет.
После того, как мы остановили скрипт, мы можем получить список 
адресов из базы данных.

Чтобы использовать несколько ВМ можно реализовать запуск нескольких 
инстансов с разными конфигами на нескольких машинах.

#Запуск

docker-compose up 

python crawler.py


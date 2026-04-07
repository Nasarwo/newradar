# newradar — Радар осадков Росгидромета

Single-File Web Application для интерактивного отображения метеорадарных данных.

## Запуск

```bash
cd server
go run .
# или
./newradar.exe
```

Откройте [http://localhost:8080](http://localhost:8080)

Клиентская часть находится в `client/static`, backend — в `server`.

## reference_map.png

Для субтракции фона используется файл `reference_map.png` в корне проекта. Если файл отсутствует, обработка выполняется без вычитания фона.

## Источник данных

- GIF: [https://meteoinfo.ru/hmc-output/rmap/phenomena.gif](https://meteoinfo.ru/hmc-output/rmap/phenomena.gif) (обновление каждые 10 мин)


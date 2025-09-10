# Crosspromo_backend_GO

Сервер, написанный на go + grcp с http ручкой для запросов с frontend.

## Ключевые моменты

-  Приложение написано на Go, используется Protocol Buffers и gRPC. 
-  Вытягивание данных из mongoDB с предварительной агрегацией для формирования необходимой структуры данных

## Стэк

-  Golang
-  Protocol Buffers
-  gRCP
-  mongoDB


## Установка и запуск

1. Клонирование репозитория

`https://github.com/polyakovWeb/crosspromo_backend_go.git`

2. Установка зависимостей

`go mod tidy`

3. Запуск приложения

`go run 'path/to/main.go'`

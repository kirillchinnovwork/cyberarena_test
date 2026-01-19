# CyberArena Platform

## Архитектура

### Сервисы

| Сервис | Порт | Описание |
|--------|------|----------|
| `gateway` | 8080 | Единый HTTP API Gateway с JWT авторизацией |
| `auth` | 50053 (gRPC) | Аутентификация и управление ролями |
| `users` | 50051 (gRPC) | Управление пользователями |
| `polygon` | 50054 (gRPC) | Полигоны, инциденты, команды, Labs |
| `news` | 50052 (gRPC) | Новости и объявления |
| `attachments` | 50055 (gRPC) | Файловые вложения |
| `external_controller` | 50056 (gRPC) | Интеграция с Jenkins/Terraform/Ansible |
| `postgres` | 5432 | PostgreSQL база данных |
| `minio` | 9000/9001 | S3-совместимое хранилище |

### Система ролей

- **user** - обычный пользователь, доступ к публичным эндпоинтам
- **admin** - администратор, доступ к `/v1/admin/*` эндпоинтам

## Быстрый старт

1. Скопируйте `.env.example` в `.env` и настройте переменные:
```bash
cp .env.example .env
```

2. Запустите сервисы:
```bash
docker-compose up -d
```

3. API доступно по адресу: http://localhost:8080

## API Endpoints

### Аутентификация

```
POST /v1/auth/register
POST /v1/auth/login
POST /v1/auth/refresh
POST /v1/auth/validate
```

### Практические задания (Labs)

```
GET  /v1/polygon/{polygon_id}/lab
GET  /v1/polygon/{polygon_id}/lab/steps

GET  /v1/admin/polygon/{polygon_id}/lab/{lab_id}/steps/{step_id}/answer
POST /v1/admin/labs
GET  /v1/admin/labs
PATCH /v1/admin/labs/{id}
DELETE /v1/admin/labs/{id}
POST /v1/admin/labs/{lab_id}/steps
```

### External Controller

```
POST /v1/admin/external/jenkins/run
POST /v1/admin/external/terraform/run
POST /v1/admin/external/ansible/run
GET  /v1/admin/external/jobs/{job_id}/status
GET  /v1/admin/external/jobs/{job_id}/logs
POST /v1/admin/external/jobs/{job_id}/cancel
GET  /v1/admin/external/jobs
```

## Структура базы данных

### Таблица `labs`
| Поле | Тип | Описание |
|------|-----|----------|
| id | uuid | Primary key |
| polygon_id | uuid | FK на полигон |
| title | text | Название задания |
| description | text | Описание |
| started_at | timestamp | Время начала |
| ttl_seconds | bigint | Время жизни (секунды) |
| group_id | uuid | FK на группу |
| step_count | int | Количество шагов |
| created_at | timestamp | Время создания |

### Таблица `lab_steps`
| Поле | Тип | Описание |
|------|-----|----------|
| id | uuid | Primary key |
| lab_id | uuid | FK на lab |
| title | text | Название шага |
| description | text | Описание |
| initial_items | jsonb | Начальные данные |
| has_answer | boolean | Есть ли ответ |
| answer | jsonb | Ответ |
| order_index | int | Порядковый индекс |

### Таблица `auth_credentials`
| Поле | Тип | Описание |
|------|-----|----------|
| user_id | uuid | Primary key |
| user_name | text | Имя пользователя |
| password_hash | text | Хеш пароля |
| role | smallint | Роль (1=user, 2=admin) |
| created_at | timestamp | Время создания |
| updated_at | timestamp | Время обновления |

## Примеры запросов

### Регистрация пользователя
```bash
curl -X POST http://localhost:8080/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username": "user1", "password": "password123", "role": 1}'
```

### Вход
```bash
curl -X POST http://localhost:8080/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"name": "user1", "password": "password123"}'
```

### Создание Lab
```bash
curl -X POST http://localhost:8080/v1/admin/labs \
  -H "Authorization: Bearer <admin_token>" \
  -H "Content-Type: application/json" \
  -d '{
    "polygon_id": "uuid-of-polygon",
    "title": "Практическое задание 1",
    "description": "Описание задания",
    "ttl_seconds": 7200
  }'
```

### Запуск Jenkins job
```bash
curl -X POST http://localhost:8080/v1/admin/external/jenkins/run \
  -H "Authorization: Bearer <admin_token>" \
  -H "Content-Type: application/json" \
  -d '{
    "job_name": "deploy-infra",
    "params": {"ENV": "staging", "VERSION": "1.0.0"}
  }'
```

## Разработка

### Генерация proto
```bash
task generate
```

### Сборка сервисов
```bash
task build
```

### Запуск тестов
```bash
task test
```

## Лицензия

MIT

-- Включаем расширение uuid-ossp для генерации UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Создаем таблицу для пользователей
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    telegram_user_id BIGINT NOT NULL UNIQUE,
    name TEXT NULL,
    language TEXT DEFAULT 'ru',
    timezone TEXT DEFAULT 'UTC',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Создаем таблицу для каналов
CREATE TABLE channels (
    id SERIAL PRIMARY KEY,
    channel_id BIGINT NOT NULL UNIQUE,
    title TEXT NULL,
    owner_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Создаем таблицу для постов
CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    channel_id INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE, -- Используем INTEGER, т.к. channels.id SERIAL
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    content TEXT NULL,
    media_type TEXT NULL,
    media_file_id TEXT NULL,
    buttons_json JSONB NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    scheduled_at TIMESTAMP WITH TIME ZONE NULL,
    job_id TEXT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Создаем таблицу для редакторов каналов
CREATE TABLE channel_editors (
    channel_id INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE, -- Используем INTEGER, т.к. channels.id SERIAL
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role TEXT NOT NULL DEFAULT 'viewer',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT channel_editors_pkey PRIMARY KEY (channel_id, user_id)
);

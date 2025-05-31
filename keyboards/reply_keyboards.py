```python
# keyboards/reply_keyboards.py

from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import ReplyKeyboardBuilder

def get_main_menu_keyboard() -> ReplyKeyboardMarkup:
    """
    Creates a reply keyboard for the main menu.
    Buttons: "➕ Новый пост", "🗂 Мои посты", "📰 Добавить RSS", "❓ Помощь".
    Layout: 2x2.
    """
    builder = ReplyKeyboardBuilder()
    builder.add(
        KeyboardButton(text="➕ Новый пост"),
        KeyboardButton(text="🗂 Мои посты"),
        KeyboardButton(text="📰 Добавить RSS"),
        KeyboardButton(text="❓ Помощь")
    )
    # Adjust layout to 2 columns
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)

def get_add_media_skip_cancel_keyboard() -> ReplyKeyboardMarkup:
    """
    Creates a reply keyboard for adding media step.
    Buttons: "Добавить медиа", "Пропустить", "❌ Отменить".
    Layout: ["Добавить медиа", "Пропустить"], ["❌ Отменить"].
    """
    builder = ReplyKeyboardBuilder()
    builder.add(
        KeyboardButton(text="Добавить медиа"),
        KeyboardButton(text="Пропустить"),
        KeyboardButton(text="❌ Отменить")
    )
    # Adjust layout to 2 columns for first two buttons, then 1 for the last
    builder.adjust(2, 1)
    return builder.as_markup(resize_keyboard=True)

def get_confirm_content_keyboard() -> ReplyKeyboardMarkup:
    """
    Creates a reply keyboard for confirming or editing content.
    Buttons: "✅ Далее", "✏️ Редактировать контент", "❌ Отменить".
    Layout: ["✅ Далее", "✏️ Редактировать контент"], ["❌ Отменить"].
    """
    builder = ReplyKeyboardBuilder()
    builder.add(
        KeyboardButton(text="✅ Далее"),
        KeyboardButton(text="✏️ Редактировать контент"),
        KeyboardButton(text="❌ Отменить")
    )
    # Adjust layout to 2 columns for first two buttons, then 1 for the last
    builder.adjust(2, 1)
    return builder.as_markup(resize_keyboard=True)

def get_channel_selection_controls_keyboard() -> ReplyKeyboardMarkup:
    """
    Creates a reply keyboard for channel selection step.
    Buttons: "Добавить ещё", "Готово", "❌ Отменить".
    Layout: ["Добавить ещё", "Готово"], ["❌ Отменить"].
    """
    builder = ReplyKeyboardBuilder()
    builder.add(
        KeyboardButton(text="Добавить ещё"),
        KeyboardButton(text="Готово"),
        KeyboardButton(text="❌ Отменить")
    )
    # Adjust layout to 2 columns for first two buttons, then 1 for the last
    builder.adjust(2, 1)
    return builder.as_markup(resize_keyboard=True)

def get_cancel_keyboard() -> ReplyKeyboardMarkup:
    """
    Creates a reply keyboard with only a cancel button.
    Button: "❌ Отменить".
    Layout: Single button.
    """
    builder = ReplyKeyboardBuilder()
    builder.add(
        KeyboardButton(text="❌ Отменить")
    )
    # No adjust needed for a single button
    return builder.as_markup(resize_keyboard=True)

# Example Usage (optional, for testing purposes):
# if __name__ == '__main__':
#     print("Main Menu Keyboard:")
#     print(get_main_menu_keyboard().keyboard)
#     print("\nAdd Media/Skip/Cancel Keyboard:")
#     print(get_add_media_skip_cancel_keyboard().keyboard)
#     print("\nConfirm Content Keyboard:")
#     print(get_confirm_content_keyboard().keyboard)
#     print("\nChannel Selection Controls Keyboard:")
#     print(get_channel_selection_controls_keyboard().keyboard)
#     print("\nCancel Keyboard:")
#     print(get_cancel_keyboard().keyboard)
```

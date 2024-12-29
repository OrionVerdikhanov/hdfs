import os
import random
import datetime
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Если у вас не установлена библиотека matplotlib/pandas:
# pip install matplotlib pandas

def generate_weather_data():
    """
    Генерирует случайные данные по температуре в выбранных городах
    за последние 30 дней.
    Возвращает Pandas DataFrame со столбцами: 
    ['date', 'city', 'temperature'].
    """
    cities = ["London", "New York", "Tokyo", "Berlin", "Moscow", "Sydney"]
    today = datetime.date.today()
    data = []

    for city in cities:
        # Собираем данные за 30 дней
        for i in range(30):
            date_i = today - datetime.timedelta(days=30 - i)
            # Генерируем случайную температуру (имитация от -5 до +35)
            temp = random.uniform(-5, 35)
            data.append([date_i, city, round(temp, 1)])

    df = pd.DataFrame(data, columns=["date", "city", "temperature"])
    return df


def plot_temperature_changes(df):
    """
    Строит график изменения температуры в разных городах.
    Сохраняет результат в 'temperature_changes.png'.
    """
    # Убедимся, что дата в формате datetime (на всякий случай)
    df["date"] = pd.to_datetime(df["date"])

    # Сортируем по дате для корректного отображения графика
    df = df.sort_values(by=["city", "date"])

    # Группируем по городам
    cities = df["city"].unique()

    plt.figure(figsize=(10, 6))
    for city in cities:
        city_data = df[df["city"] == city]
        plt.plot(city_data["date"], city_data["temperature"], label=city)

    plt.title("Изменение температуры по датам (последние 30 дней)")
    plt.xlabel("Дата")
    plt.ylabel("Температура (°C)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("temperature_changes.png")
    plt.close()

    print("[INFO] График изменений температуры сохранён в 'temperature_changes.png'.")


def plot_temperature_distribution(df):
    """
    Строит гистограмму распределения температур для всех городов вместе.
    Сохраняет результат в 'temperature_distribution.png'.
    """
    plt.figure(figsize=(8, 5))
    plt.hist(df["temperature"], bins=20, edgecolor="black")
    plt.title("Распределение температуры (за 30 дней, все города)")
    plt.xlabel("Температура (°C)")
    plt.ylabel("Частота")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("temperature_distribution.png")
    plt.close()

    print("[INFO] Гистограмма распределения температур сохранена в 'temperature_distribution.png'.")


def save_results_to_hdfs(local_path, hdfs_path):
    """
    Сохраняет файл local_path в HDFS по пути hdfs_path.
    Использует системную команду `hadoop fs -put`.
    
    Пример использования:
        save_results_to_hdfs('weather_data.csv', '/user/username/weather_data.csv')
    """
    cmd = f"hadoop fs -put -f {local_path} {hdfs_path}"
    print(f"[INFO] Выполняется: {cmd}")
    os.system(cmd)


def get_results_from_hdfs(hdfs_path, local_path):
    """
    Выгружает файл из HDFS (hdfs_path) на локальный компьютер (local_path).
    Использует системную команду `hadoop fs -get`.
    
    Пример использования:
        get_results_from_hdfs('/user/username/weather_data.csv', 'weather_data_local.csv')
    """
    cmd = f"hadoop fs -get -f {hdfs_path} {local_path}"
    print(f"[INFO] Выполняется: {cmd}")
    os.system(cmd)


def main():
    # 1. Сбор данных (в нашем примере - генерация)
    df_weather = generate_weather_data()
    print("[INFO] Данные по погоде (имитация) собраны.")

    # Сохраняем в CSV локально
    csv_file = "weather_data.csv"
    df_weather.to_csv(csv_file, index=False)
    print(f"[INFO] Исходные данные сохранены в '{csv_file}'.")

    # 2. Построение графиков
    plot_temperature_changes(df_weather)
    plot_temperature_distribution(df_weather)

    # 3. Сохранение результатов (csv и рисунки) в HDFS 
    #    (путь и имя в HDFS меняйте в зависимости от вашей конфигурации)
    hdfs_csv_path = "/user/your_hdfs_user/weather_data.csv"
    hdfs_temp_changes = "/user/your_hdfs_user/temperature_changes.png"
    hdfs_temp_distribution = "/user/your_hdfs_user/temperature_distribution.png"

    save_results_to_hdfs(csv_file, hdfs_csv_path)
    save_results_to_hdfs("temperature_changes.png", hdfs_temp_changes)
    save_results_to_hdfs("temperature_distribution.png", hdfs_temp_distribution)

    # 4. Пример выгрузки результатов из HDFS на локальный компьютер
    #    (из HDFS обратно к себе)
    Path("results_from_hdfs").mkdir(exist_ok=True)
    get_results_from_hdfs(hdfs_csv_path, "results_from_hdfs/weather_data.csv")
    get_results_from_hdfs(hdfs_temp_changes, "results_from_hdfs/temperature_changes.png")
    get_results_from_hdfs(hdfs_temp_distribution, "results_from_hdfs/temperature_distribution.png")

    print("[INFO] Процесс завершён.")


if __name__ == "__main__":
    main()

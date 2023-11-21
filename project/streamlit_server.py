import datetime

import streamlit as st
from matplotlib import pyplot as plt

import time

import xmlrpc.client

statistics_server_client: xmlrpc.client.ServerProxy


def init_statistic_server_client():
    global statistics_server_client
    try:
        print("Try connect to statistic server")
        statistics_server_client = xmlrpc.client.ServerProxy("http://localhost:8018")
        print("Success connection to statistic server")
    except():
        print("Fail to connect to statistic server")


def convert_str_datetime_to_timestamp(str_datetime):
    return int(datetime.datetime.timestamp(datetime.datetime.strptime(str_datetime, "%Y-%m-%d %H:%M:%S")))


def get_timestamp_from_date(date_input: datetime.date):
    return int(datetime.datetime.fromisoformat(date_input.isoformat()).timestamp())


def filter_logs_by_period(logs, start_timestamp, end_timestamp):
    result = list()
    for log in logs:
        log_timestamp = convert_str_datetime_to_timestamp(log[2])
        if start_timestamp <= log_timestamp <= end_timestamp:
            result.append(log)
    return result


def group_logs_by_operation(logs):
    result = dict()
    for log in logs:
        operation = log[1]
        if operation in result:
            result[operation].append(log)
        else:
            operation_logs = list()
            operation_logs.append(log)
            result[operation] = operation_logs
    return result


def count_log_by_operation(log_by_operations):
    result = dict()
    for operation in log_by_operations.keys():
        result[operation] = len(log_by_operations[operation])
    return result


def get_days_list(start_date, end_date):
    result = list()
    day = start_date
    while day <= end_date:
        result.append(day)
        day += datetime.timedelta(days=1)
    return result


def get_logs_list_by_operation_type(grouped_by_operation_logs, operation_type):
    if operation_type in grouped_by_operation_logs.keys():
        return grouped_by_operation_logs[operation_type]
    else:
        return list()


def split_day_by_interval(day, interval_hours):
    interval_timestamp = interval_hours * 60 * 60
    next_day = day + datetime.timedelta(days=1)
    next_day_timestamp = get_timestamp_from_date(next_day)
    day_timestamp = get_timestamp_from_date(day)
    day_timestamps = list()
    while day_timestamp < next_day_timestamp:
        day_timestamps.append(day_timestamp)
        day_timestamp = day_timestamp + interval_timestamp
    return day_timestamps, interval_timestamp


def get_logs_count_by_day_split_by_interval(grouped_by_operation_logs, days, interval):
    flatten_logs_values = grouped_by_operation_logs
    if type(grouped_by_operation_logs) is dict:
        flatten_logs_values = [item for sublist in list(grouped_by_operation_logs.values()) for item in sublist]
    print(flatten_logs_values)
    result = dict()
    for day in days:
        day_timestamps, interval_timestamp = split_day_by_interval(day, interval)
        timestamp_logs_count = list()
        for current_timestamp in day_timestamps:
            next_timestamp = current_timestamp + interval_timestamp
            logs_count = len(
                list(filter(lambda it: current_timestamp < convert_str_datetime_to_timestamp(it[2]) <= next_timestamp,
                            flatten_logs_values)))
            timestamp_logs_count.append({"timestamp": next_timestamp, "count": logs_count})
        result[day] = timestamp_logs_count
    return result


def get_hour_from_timestamp(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).hour


def get_flat_by_day_logs_count_split_by_interval(logs_count_by_day_split_by_interval):
    result = dict()
    for day in logs_count_by_day_split_by_interval.keys():
        for timestamp_count in logs_count_by_day_split_by_interval[day]:
            hour = get_hour_from_timestamp(timestamp_count['timestamp'])
            if hour not in result.keys():
                result[hour] = 0
            result[hour] += timestamp_count['count']
    return result


def get_average_from_list(source):
    return sum(source) / len(source)


def get_time_from_str(source_str):
    return datetime.datetime.strptime(source_str, "%H:%M:%S.%f")


def convert_time_to_ms(source_time: datetime.time):
    return round(source_time.hour * 60 * 60 * 1000 + source_time.minute * 60 * 1000
                 + source_time.second * 1000 + (source_time.microsecond / 1000))


def get_avg_duration_by_operation(grouped_by_operation_logs):
    result = dict()
    for operation in grouped_by_operation_logs.keys():
        logs = grouped_by_operation_logs[operation]
        result[operation] = get_average_from_list(
            list(map(lambda it: convert_time_to_ms(get_time_from_str(it[3]).time()), logs)))
    return result


init_statistic_server_client()

st.write("# Работа веб-сервиса")


def main():
    # Настройка боковой панели
    st.sidebar.header("Параметры выборки")
    st.sidebar.subheader("Период выборки")
    now = datetime.date.today()
    select_start_date = st.sidebar.date_input("Начало", value=now, max_value=now)
    select_end_date = st.sidebar.date_input("Конец", value=now + datetime.timedelta(days=1))
    day_inteval = st.sidebar.slider("Интервал", min_value=1, max_value=24, value=24)
    selected_operation = st.sidebar.selectbox('Операция',
                                              list(map(lambda it: it[0],
                                                       statistics_server_client.get_operations_types())))

    # Подготовка дат для выборки
    select_start_timestamp = get_timestamp_from_date(select_start_date)
    select_end_timestamp = get_timestamp_from_date(select_end_date)

    # Подготовка коллекций для диаграмм
    logs = statistics_server_client.get_all_logs()
    logs = filter_logs_by_period(logs, select_start_timestamp, select_end_timestamp)
    grouped_by_operation_logs = group_logs_by_operation(logs)
    logs_count_by_operation = count_log_by_operation(grouped_by_operation_logs)
    days = get_days_list(select_start_date, select_end_date)

    all_logs_count_split_by_timestamp_interval = get_logs_count_by_day_split_by_interval(grouped_by_operation_logs,
                                                                                         days, day_inteval)
    all_logs_count_split_by_timestamp_interval_flat_by_day = get_flat_by_day_logs_count_split_by_interval(
        all_logs_count_split_by_timestamp_interval)

    logs_by_type = get_logs_list_by_operation_type(grouped_by_operation_logs, selected_operation)
    logs_by_type_count_split_by_timestamp_interval = get_logs_count_by_day_split_by_interval(logs_by_type, days,
                                                                                             day_inteval)
    logs_by_type_count_split_by_timestamp_interval_flat_by_day = get_flat_by_day_logs_count_split_by_interval(
        logs_by_type_count_split_by_timestamp_interval)

    average_duration_by_operation = get_avg_duration_by_operation(grouped_by_operation_logs)

    # Диаграмма - количество логов по всем типам
    st.subheader('Количество логов по всем типам за период')
    st.bar_chart(logs_count_by_operation)

    # Диаграмма - количество логов по всем типам, разбитых по часовым интервалам за период
    st.subheader('Количество логов по часам по всем типам')
    st.bar_chart(all_logs_count_split_by_timestamp_interval_flat_by_day)

    # Диаграмма - количество логов по выбранному тип, разбитых по часовым интервалам за период
    st.subheader('Количество логов по часам по типу операции - ' + selected_operation)
    st.bar_chart(logs_by_type_count_split_by_timestamp_interval_flat_by_day)

    # Диаграмма - количество логов по типу операции за период (круговая)
    fig, ax = plt.subplots()
    ax.pie(logs_count_by_operation.values(), labels=logs_count_by_operation.keys())
    st.subheader("Количество логов по типу операции за период")
    st.pyplot(fig)

    print(average_duration_by_operation)

    # Диаграмма - среднее время выполнения операции
    st.subheader("Среднее время выполнения операции")
    fig, ax = plt.subplots()
    ax.pie(average_duration_by_operation.values(), labels=average_duration_by_operation.keys())
    st.pyplot(fig)


if __name__ == "__main__":
    main()

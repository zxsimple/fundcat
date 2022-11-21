import requests
import json
import datetime
import os
import pandas as pd
import argparse

def query_top_rank(rank_date, interval_days):
    """
    Query and download top ranking fund in specified duration, save the results in CSV format

    :param rank_date: the query date
    :param interval_days: the interval duration between query date
    """
    end_date = datetime.datetime.strptime(rank_date, '%Y-%m-%d') + datetime.timedelta(days=float(interval_days))
    end_date = end_date.strftime('%Y-%m-%d')

    url = _get_query_url().format(rank_date, end_date)
    headers = _get_request_header()

    json_response = requests.get(url, headers=headers)

    # Clean the data
    raw_json = json_response.text.replace("var rankData = ", "")
    end_pos = raw_json.index("allRecords")
    raw_json = raw_json[:end_pos - 1] + "}"
    raw_json = raw_json.replace("datas", "\"datas\"")
    # End of cleaning the data

    # top_funds = json.loads(raw_json, encoding="utf-8")["datas"]
    top_funds = json.loads(raw_json)["datas"]

    with open(os.path.join("data", str(interval_days), "{}.csv".format(rank_date)), "w") as f:
        lines = [_get_dataset_header(), os.linesep]
        for fund in top_funds:
            fund_id = fund.split(",")[0]
            name = fund.split(",")[1]
            start_price = fund.split(",")[7]
            end_price = fund.split(",")[10]
            growth_ratio = fund.split(",")[3]
            lines.append("{}\t{}\t{}\t{}\t{}".format(fund_id, name.encode('utf-8'), start_price, end_price,
                                                     growth_ratio))
            lines.append(os.linesep)
        f.writelines(lines)


def rank_distribution(query_date, interval_days, period):
    """
    Statistics days distribution for top fund ranking

    :param query_date: The query date for distribution analysis
    :param interval_days: The ranking interval
    :param period: Period to be analyzed
    :return:
    """

    data_files = []

    for x in range(period):
        d = datetime.datetime.strptime(query_date, '%Y-%m-%d') + datetime.timedelta(days=float(x))
        data_files.append(os.path.join("data", str(interval_days), "{}.csv".format(d.strftime('%Y-%m-%d'))))
    df = pd.concat([pd.read_csv(f, sep="\t", dtype={"ID": object}) for f in data_files], ignore_index=True, sort=False)

    top_count_df = df.groupby('ID').agg({'ID': 'count'}).nlargest(50, columns=['ID'])

    top_growth_ratio_df = df.groupby('ID').agg({'GROWTH_RATIO': 'mean'}).nlargest(50, columns=['GROWTH_RATIO'])
    print ("-" * 30)
    print (top_count_df)
    print ("-" * 30)
    print (top_growth_ratio_df)
    print ("-" * 30)

def _get_request_header():
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/91.0.4472.114 Safari/537.36",
        "Referer": "http://fund.eastmoney.com/data/fundranking.html",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Cookie": "qgqp_b_id=1fc5c7ff3ad1d7c8b42377e601995367; st_si=30919754188034; st_asi=delete; "
                  "ASP.NET_SessionId=szsa4rlq3qg14nevxecnjqkq; st_pvi=96965586997253; st_sp=2021-06-20%2019%3A50%3A35; "
                  "st_inirUrl=http%3A%2F%2Flink.zhihu.com%2F; st_sn=5; st_psi=20210630195834974-112200312936-0699058977"
    }


def _get_query_url():
    return "http://fund.eastmoney.com/data/rankhandler.aspx?op=dy&dt=kf&ft=all&rs=&gs=0&sc=qjzf&st=desc&" \
           "sd={}&ed={}&es=0&qdii=&pi=1&pn=50&dx=0&v=0.8435995375037855"


def _get_dataset_header():
    return "{}\t{}\t{}\t{}\t{}".format("ID", "NAME", "START_PRICE", "END_PRICE", "GROWTH_RATIO")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Demo of argparse')
    
    since_date = datetime.datetime.now() + datetime.timedelta(days=float(-30 - 2))
    since = since_date.strftime('%Y-%m-%d')

    # 2. 添加命令行参数
    parser.add_argument('--since', type=str, default=since)
    parser.add_argument('--interval_days', type=int, default=7)
    parser.add_argument('--period', type=int, default=14)
    parser.add_argument('--download', type=bool, default=False)
    
    # 3. 从命令行中结构化解析参数
    args = parser.parse_args()
    print(args)

    since = args.since
    interval_days = args.interval_days
    period = args.period
    download = args.download

    print('Since {}, query 50 funds with top performance in last {} days, anaylyze their statistics within period of {} days.'
    .format(since, interval_days, period))
    new_year = '2022-10-01'
    interval = 7

    if download:
        for i in range(365):
            date = datetime.datetime.strptime(since, '%Y-%m-%d') + datetime.timedelta(days=float(i))
            if date < datetime.datetime.now() + datetime.timedelta(days=float(-2)):
                date = date.strftime('%Y-%m-%d')
                print("retrieve top 50 funds in last {} days on {}.".format(interval, date))
                query_top_rank(date, interval_days)

    rank_distribution(since, interval_days, period)

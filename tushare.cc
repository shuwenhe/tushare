#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <ctime>
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <clickhouse/client.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>

using json = nlohmann::json;
using namespace clickhouse;

// ---------------- 数据结构 ----------------
struct StockDay {
    std::string ts_code;
    std::string trade_date;
    double open, high, low, close, pre_close, change, pct_chg, vol, amount;
    double date_stamp;
};

// ---------------- 工具函数 ----------------
size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

double cover_time(const std::string& date_str) {
    struct tm tm = {0};
    if (strptime(date_str.c_str(), "%Y%m%d", &tm) == nullptr) return 0;
    return (double)mktime(&tm);
}

// ---------------- 核心同步类 ----------------
class TushareSync {
public:
    TushareSync(std::string ts_token, const std::string& ch_host) : token(ts_token) {
        ClientOptions options;
        options.SetHost(ch_host).SetUser("quant").SetPassword("quant").SetDefaultDatabase("quant");
        ch_client = std::make_unique<Client>(options);
    }

    void sync_daily(std::string ts_code, std::string start_date, std::string end_date) {
        std::string url = "https://api.tushare.pro";
        json body = {
            {"api_name", "daily"},
            {"token", token},
            {"params", {{"ts_code", ts_code}, {"start_date", start_date}, {"end_date", end_date}}}
        };

        std::string resp_str = post_request(url, body.dump());
        auto resp_json = json::parse(resp_str);

        if (resp_json["code"] != 0) {
            std::cerr << "Tushare Error: " << resp_json["msg"] << std::endl;
            return;
        }

        auto& data_part = resp_json["data"];
        auto& items = data_part["items"];
        
        std::vector<StockDay> batch;
        for (auto& row : items) {
            StockDay sd;
            sd.ts_code    = row[0].get<std::string>();
            sd.trade_date = row[1].get<std::string>();
            sd.open       = row[2].get<double>();
            sd.high       = row[3].get<double>();
            sd.low        = row[4].get<double>();
            sd.close      = row[5].get<double>();
            sd.pre_close  = row[6].get<double>();
            sd.change     = row[7].get<double>();
            sd.pct_chg    = row[8].get<double>();
            sd.vol        = row[9].get<double>();
            sd.amount     = row[10].get<double>();
            sd.date_stamp = cover_time(sd.trade_date);
            batch.push_back(sd);
        }

        write_to_clickhouse(batch);
    }

private:
    std::string token;
    std::unique_ptr<Client> ch_client;

    std::string post_request(const std::string& url, const std::string& json_payload) {
        CURL* curl = curl_easy_init();
        std::string buffer;
        if (curl) {
            struct curl_slist* headers = nullptr;
            headers = curl_slist_append(headers, "Content-Type: application/json");
            curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_payload.c_str());
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buffer);
            curl_easy_perform(curl);
            curl_easy_cleanup(curl);
        }
        return buffer;
    }

    void write_to_clickhouse(const std::vector<StockDay>& batch) {
        if (batch.empty()) return;

        Block block;
        auto c_code = std::make_shared<ColumnString>();
        auto c_date = std::make_shared<ColumnString>();
        auto c_open = std::make_shared<ColumnFloat64>();
        auto c_high = std::make_shared<ColumnFloat64>();
        auto c_low = std::make_shared<ColumnFloat64>();
        auto c_close = std::make_shared<ColumnFloat64>();
        auto c_vol = std::make_shared<ColumnFloat64>();
        auto c_stamp = std::make_shared<ColumnFloat64>();

        for (auto& d : batch) {
            c_code->Append(d.ts_code);
            c_date->Append(d.trade_date);
            c_open->Append(d.open);
            c_high->Append(d.high);
            c_low->Append(d.low);
            c_close->Append(d.close);
            c_vol->Append(d.vol);
            c_stamp->Append(d.date_stamp);
        }

        block.AppendColumn("ts_code", c_code);
        block.AppendColumn("trade_date", c_date);
        block.AppendColumn("open", c_open);
        block.AppendColumn("high", c_high);
        block.AppendColumn("low", c_low);
        block.AppendColumn("close", c_close);
        block.AppendColumn("vol", c_vol);
        block.AppendColumn("date_stamp", c_stamp);

        ch_client->Insert("stock_day_data", block);
        std::cout << "Successfully inserted " << batch.size() << " rows." << std::endl;
    }
};

int main() {
    // 替换为你的真实 Token
    TushareSync sync("YOUR_TUSHARE_TOKEN_HERE", "127.0.0.1");

    std::cout << "Starting sync for 000001.SZ (平安银行)..." << std::endl;
    sync.sync_daily("000001.SZ", "20240101", "20241231");

    return 0;
}

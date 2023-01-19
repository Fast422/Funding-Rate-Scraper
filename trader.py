import json, asyncio, aiohttp, pandas as pd, requests as r, time

class scraper:

    def printMostNegative(self, df):
        mostNegative = df.head(3)
        print("Most Negative:")
        print(mostNegative)
    
    def printMostPositive(self, df):
        mostPositive = df.tail(3).sort_values(by="Funding (Decimal)", ascending=False)
        print("Most Positive:")
        print(mostPositive)

class bybitBinanceBitmex(scraper):

    def getFundingRate(self, exchange=["binance", "bybit", "bitmex"]):
        if exchange == "binance":
            url = "https://fapi.binance.com/fapi/v1/premiumIndex"
        elif exchange == "bybit":
            url = "https://api.bybit.com/v2/public/tickers"
        elif exchange == "bitmex":
            url = "https://www.bitmex.com/api/v1/instrument/active"

        request = r.get(url)
        print(request.text)
        js0 = json.loads(request.text)
        #print(js0)
        df = pd.DataFrame(js0)
        df = df[["symbol", "lastFundingRate"]].copy()
        df.rename(columns={"symbol": "Symbol", "lastFundingRate": "Funding (Decimal)"}, inplace=True)
        df["Funding (Percentage)"] = df["Funding (Decimal)"].astype(float) * 100
        df["Funding (Decimal)"] = df["Funding (Decimal)"].astype(float)
        df.sort_values(by="Funding (Decimal)", inplace=True)
        df = df[df["Funding (Decimal)"].notna()]
        return df
    
class mexcBitget(scraper):

    def __init__(self, exchange=["mexc", "bitget"]):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        self.exchange = exchange
       
        if self.exchange == "mexc":
            self.coinListURL = "https://contract.mexc.com/api/v1/contract/risk_reverse"
        elif self.exchange == "bitget":
            self.coinListURL = "https://api.bitget.com/api/mix/v1/market/contracts?productType=umcbl"
        self.coinList = asyncio.run(self.getcoinList())

    async def getcoinList(self):

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url=self.coinListURL) as response:
                    resp = await response.read()
                    data = json.loads(resp.decode('utf-8'))["data"]
                    coinList = [x["symbol"] for x in data]
                    return coinList
        except Exception as e:
            print("Unable to get url {} due to {}.".format(self.coinListURL, e.__class__))
    
    async def get(self, coin, session):        

        if self.exchange == "mexc":
            url = "https://contract.mexc.com/api/v1/contract/funding_rate/{}".format(coin)
        elif self.exchange == "bitget":
            url = "https://api.bitget.com/api/mix/v1/market/current-fundRate?symbol={}".format(coin)
        try:
            async with session.get(url=url) as response:
                resp = await response.read()
                return json.loads(resp.decode('utf-8'))["data"]  
        except Exception as e:
            print("Unable to get url {} due to {}.".format(url, e.__class__))
    
    async def getFundingRate(self):
        
        if self.exchange == "mexc":
            batchLimit = 10
        elif self.exchange == "bitget":
            batchLimit = 20

        async with aiohttp.ClientSession() as session:
            data = []
            for i in range(0, len(self.coinList), batchLimit):
                result = await asyncio.gather(*[self.get(coin, session) for coin in self.coinList[i:i+batchLimit]])
                result = filter(lambda x: x is not None, result)
                #print(result)
                print(f"Finalized batch of {batchLimit}.")
                data += result
                await asyncio.sleep(1)
            
        df = pd.DataFrame(data, columns=["symbol", "fundingRate"]).rename(columns={"symbol": "Symbol", "fundingRate": "Funding (Decimal)"})
        df["Funding (Percentage)"] = df["Funding (Decimal)"].astype(float) * 100
        df.sort_values(by="Funding (Percentage)", inplace=True)
        
        return df

class mexcFundingScraper(mexcBitget):

    def __init__(self):
        super().__init__(exchange="mexc")

class bitgetFundingScraper(mexcBitget):
    
    def __init__(self):
        super().__init__(exchange="bitget")

class binanceFundingScraper(bybitBinanceBitmex):
    
    def getFundingRate(self):
        return bybitBinanceBitmex.getFundingRate(self, exchange="binance")

class bitmexFundingScraper(scraper):
    
    def getFundingRate(self):
        return bybitBinanceBitmex.getFundingRate(self, exchange="bitmex")

class bybitFundingScraper(scraper):

    def getFundingRate(self):
        return bybitBinanceBitmex.getFundingRate(self, exchange="bybit")

class fundingScraper(scraper):

    def __init__(self):
        exchange_scrapers = {
            "1": bybitFundingScraper,
            "2": binanceFundingScraper,
            "3": bitmexFundingScraper,
            "4": bitgetFundingScraper,
            "5": mexcFundingScraper
        }
        exchange = "0"
        while exchange not in exchange_scrapers:
            exchange = input("""Enter exchange:
            1. Bybit
            2. Binance
            3. Bitmex
            4. Bitget
            5. MEXC
            """)

        scraper = exchange_scrapers[exchange]()
        print(type(scraper))
        
        if exchange in ["1", "2", "3"]:
            df = scraper.getFundingRate()
            #Only works for binance cause I need to figure out how to filter the data properly for the other exchanges
        else:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            df = asyncio.run(scraper.getFundingRate())
        print(df)
        scraper.printMostNegative(df)
        scraper.printMostPositive(df)


def main():
    fundingScraper()

if __name__ == "__main__":
    main()

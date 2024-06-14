#ifndef INCLUDE_CCAPI_CPP_SERVICE_CCAPI_MARKET_DATA_SERVICE_HYPERLIQUID_H_
#define INCLUDE_CCAPI_CPP_SERVICE_CCAPI_MARKET_DATA_SERVICE_HYPERLIQUID_H_
#ifdef CCAPI_ENABLE_SERVICE_MARKET_DATA
#ifdef CCAPI_ENABLE_EXCHANGE_HYPERLIQUID
#include "ccapi_cpp/service/ccapi_market_data_service.h"
namespace ccapi {
class MarketDataServiceHyperliquid : public MarketDataService {
 public:
  MarketDataServiceHyperliquid(std::function<void(Event& , Queue<Event>*)> eventHandler, SessionOptions sessionOptions, SessionConfigs sessionConfigs, ServiceContext* serviceContextPtr): MarketDataService(eventHandler, sessionOptions, sessionConfigs, serviceContextPtr) {
    this->exchangeName = CCAPI_EXCHANGE_NAME_HYPERLIQUID;
    this->baseUrlWs = sessionConfigs.getUrlWebsocketBase().at(this->exchangeName) + "/ws";
    this->baseUrlRest = sessionConfigs.getUrlRestBase().at(this->exchangeName);
    this->setHostRestFromUrlRest(this->baseUrlRest);
    this->setHostWsFromUrlWs(this->baseUrlWs);
    this->getInstrumentsTarget = "/info";
  }

 private:

  std::vector<std::string> createSendStringList(const WsConnection& wsConnection) override {
    std::vector<std::string> sendStringList;

    for (const auto& subscriptionListByChannelIdSymbolId : this->subscriptionListByConnectionIdChannelIdSymbolIdMap.at(wsConnection.id)) {
      auto channelId = subscriptionListByChannelIdSymbolId.first;
      for (const auto& subscriptionListBySymbolId : subscriptionListByChannelIdSymbolId.second) {
        std::string symbolId = subscriptionListBySymbolId.first;
        std::string exchangeSubscriptionId = channelId + ":" + symbolId;

        if (channelId == "l2Book" || channelId == "trades" || channelId == "candle") {
          if (channelId == "l2Book") {
            this->l2UpdateIsReplaceByConnectionIdChannelIdSymbolIdMap[wsConnection.id][channelId][symbolId] = true;
          }

          rj::Document document;
          document.SetObject();
          rj::Document::AllocatorType& allocator = document.GetAllocator();

          document.AddMember("method", rj::Value("subscribe").Move(), allocator);

          rj::Value subscription(rj::kObjectType);
          subscription.AddMember("type", rj::Value(channelId.c_str(), allocator).Move(), allocator);
          subscription.AddMember("coin", rj::Value(symbolId.c_str(), allocator).Move(), allocator);

          document.AddMember("subscription", subscription, allocator);

          rj::StringBuffer stringBuffer;
          rj::Writer<rj::StringBuffer> writer(stringBuffer);
          document.Accept(writer);
          std::string sendString = stringBuffer.GetString();
          sendStringList.push_back(sendString);

          this->channelIdSymbolIdByConnectionIdExchangeSubscriptionIdMap[wsConnection.id][exchangeSubscriptionId][CCAPI_CHANNEL_ID] = channelId;
          this->channelIdSymbolIdByConnectionIdExchangeSubscriptionIdMap[wsConnection.id][exchangeSubscriptionId][CCAPI_SYMBOL_ID] = symbolId;
        }
      }
    }

    return sendStringList;
  }

  void processTextMessage(
#ifdef CCAPI_LEGACY_USE_WEBSOCKETPP
      WsConnection& wsConnection, wspp::connection_hdl hdl, const std::string& textMessage
#else
      std::shared_ptr<WsConnection> wsConnectionPtr, boost::beast::string_view textMessageView
#endif
      ,
      const TimePoint& timeReceived, Event& event, std::vector<MarketDataMessage>& marketDataMessageList) override {
#ifdef CCAPI_LEGACY_USE_WEBSOCKETPP
#else
    WsConnection& wsConnection = *wsConnectionPtr;
    std::string textMessage(textMessageView);
#endif
    rj::Document document;
    document.Parse<rj::kParseNumbersAsStringsFlag>(textMessage.c_str());

    if (document.HasMember("channel") && document["channel"].IsString()) {
      std::string channel = document["channel"].GetString();
      if (channel == "subscriptionResponse") {
        // Extract channelId and symbolId from the subscription response
        const rj::Value& data = document["data"];
        const rj::Value& subscription = data["subscription"];
        std::string channelId = subscription["type"].GetString();
        std::string symbolId = subscription["coin"].GetString();
        std::string exchangeSubscriptionId = channelId + ":" + symbolId;

        // Handle subscription response
        event.setType(Event::Type::SUBSCRIPTION_STATUS);
        std::vector<Message> messageList;
        Message message;
        message.setTimeReceived(timeReceived);
        std::vector<std::string> correlationIdList;
        if (this->correlationIdListByConnectionIdChannelIdSymbolIdMap.find(wsConnection.id) !=
            this->correlationIdListByConnectionIdChannelIdSymbolIdMap.end()) {
          if (this->correlationIdListByConnectionIdChannelIdSymbolIdMap.at(wsConnection.id).find(channelId) !=
              this->correlationIdListByConnectionIdChannelIdSymbolIdMap.at(wsConnection.id).end()) {
            if (this->correlationIdListByConnectionIdChannelIdSymbolIdMap.at(wsConnection.id).at(channelId).find(symbolId) !=
                this->correlationIdListByConnectionIdChannelIdSymbolIdMap.at(wsConnection.id).at(channelId).end()) {
              std::vector<std::string> correlationIdList_2 =
                  this->correlationIdListByConnectionIdChannelIdSymbolIdMap.at(wsConnection.id).at(channelId).at(symbolId);
              correlationIdList.insert(correlationIdList.end(), correlationIdList_2.begin(), correlationIdList_2.end());
            }
          }
        }
        message.setCorrelationIdList(correlationIdList);
        message.setType(Message::Type::SUBSCRIPTION_STARTED);
        Element element;
        element.insert(CCAPI_INFO_MESSAGE, textMessage);
        message.setElementList({element});
        messageList.emplace_back(std::move(message));
        event.setMessageList(messageList);      
      } else if (channel == "error") {
        event.setType(Event::Type::SUBSCRIPTION_STATUS);
        std::vector<Message> messageList;
        Message message;
        message.setTimeReceived(timeReceived);
        message.setType(Message::Type::SUBSCRIPTION_FAILURE);
        Element element;
        element.insert(CCAPI_ERROR_MESSAGE, textMessage);
        message.setElementList({element});
        messageList.emplace_back(std::move(message));
        event.setMessageList(messageList);
      } else {
        const rj::Value& data = document["data"];
        std::string coin;
        if (channel == "trades") {
          coin = data[0]["coin"].GetString();
        } else if (channel == "l2Book") {
          coin = data["coin"].GetString();
        }


        std::string exchangeSubscriptionId = channel + ":" + coin;
        const std::string& channelId =
            this->channelIdSymbolIdByConnectionIdExchangeSubscriptionIdMap.at(wsConnection.id).at(exchangeSubscriptionId).at(CCAPI_CHANNEL_ID);
        const std::string& symbolId =
            this->channelIdSymbolIdByConnectionIdExchangeSubscriptionIdMap.at(wsConnection.id).at(exchangeSubscriptionId).at(CCAPI_SYMBOL_ID);

        if (channel == "trades") {
          const rj::Value& data = document["data"];
          for (const auto& trade : data.GetArray()) {
            MarketDataMessage marketDataMessage;
            marketDataMessage.type = MarketDataMessage::Type::MARKET_DATA_EVENTS_TRADE;
            marketDataMessage.exchangeSubscriptionId = exchangeSubscriptionId;
            marketDataMessage.recapType = MarketDataMessage::RecapType::NONE;
            marketDataMessage.tp = TimePoint(std::chrono::milliseconds(std::stoll(trade["time"].GetString())));

            MarketDataMessage::TypeForDataPoint dataPoint;
            dataPoint.insert({MarketDataMessage::DataFieldType::PRICE, UtilString::normalizeDecimalString(std::string(trade["px"].GetString()))});
            dataPoint.insert({MarketDataMessage::DataFieldType::SIZE, UtilString::normalizeDecimalString(std::string(trade["sz"].GetString()))});
            dataPoint.insert({MarketDataMessage::DataFieldType::TRADE_ID, std::string(trade["tid"].GetString())});
            dataPoint.insert({MarketDataMessage::DataFieldType::IS_BUYER_MAKER, std::string(trade["side"].GetString()) == "B" ? "1" : "0"});

            marketDataMessage.data[MarketDataMessage::DataType::TRADE].emplace_back(std::move(dataPoint));
            marketDataMessageList.emplace_back(std::move(marketDataMessage));
          }
        } else if (channel == "l2Book") {
          MarketDataMessage marketDataMessage;
          marketDataMessage.type = MarketDataMessage::Type::MARKET_DATA_EVENTS_MARKET_DEPTH;
          marketDataMessage.exchangeSubscriptionId = exchangeSubscriptionId;
          if (this->processedInitialSnapshotByConnectionIdChannelIdSymbolIdMap[wsConnection.id][channelId][symbolId]) {
            marketDataMessage.recapType = MarketDataMessage::RecapType::NONE;
          } else {
            marketDataMessage.recapType = MarketDataMessage::RecapType::SOLICITED;
          }
          marketDataMessage.tp = TimePoint(std::chrono::milliseconds(std::stoll(document["data"]["time"].GetString())));

          // std::string coin = document["data"]["coin"].GetString();
          const rj::Value& bids = document["data"]["levels"][0];
          const rj::Value& asks = document["data"]["levels"][1];
          
          auto optionMap = this->optionMapByConnectionIdChannelIdSymbolIdMap[wsConnection.id][channelId][symbolId];
          int maxMarketDepth = std::stoi(optionMap.at(CCAPI_MARKET_DEPTH_MAX));
          int bidIndex = 0;
          for (const auto& bid : bids.GetArray()) {
            if (bidIndex >= maxMarketDepth) {
              break;
            }
            MarketDataMessage::TypeForDataPoint dataPoint;
            dataPoint.insert({MarketDataMessage::DataFieldType::PRICE, UtilString::normalizeDecimalString(bid["px"].GetString())});
            dataPoint.insert({MarketDataMessage::DataFieldType::SIZE, UtilString::normalizeDecimalString(bid["sz"].GetString())});
            marketDataMessage.data[MarketDataMessage::DataType::BID].emplace_back(std::move(dataPoint));
            ++bidIndex;
          }


          int askIndex = 0;
          for (const auto& ask : asks.GetArray()) {
            if (askIndex >= maxMarketDepth) {
              break;
            }
            MarketDataMessage::TypeForDataPoint dataPoint;
            dataPoint.insert({MarketDataMessage::DataFieldType::PRICE, UtilString::normalizeDecimalString(ask["px"].GetString())});
            dataPoint.insert({MarketDataMessage::DataFieldType::SIZE, UtilString::normalizeDecimalString(ask["sz"].GetString())});
            marketDataMessage.data[MarketDataMessage::DataType::ASK].emplace_back(std::move(dataPoint));
            ++askIndex;
          }

          marketDataMessageList.push_back(std::move(marketDataMessage));
        }
      }
    }
  }
  
  void convertRequestForRest(http::request<http::string_body>& req, const Request& request, const TimePoint& now, const std::string& symbolId, const std::map<std::string, std::string>& credential) override {
    // Convert REST requests to Hyperliquid's API format
    // ...
  }

  void convertTextMessageToMarketDataMessage(const Request& request, const std::string& textMessage, const TimePoint& timeReceived, Event& event, std::vector<MarketDataMessage>& marketDataMessageList) override {
    // Convert REST responses to MarketDataMessage format
    // ...
  }

  // Implement additional helper functions and WebSocket post request handling
  // ...
};
} /* namespace ccapi */
#endif
#endif
#endif  // INCLUDE_CCAPI_CPP_SERVICE_CCAPI_MARKET_DATA_SERVICE_

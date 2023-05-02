#include <sys/stat.h>

#include "app/common.h"
#include "app/event_handler_base.h"
#include "ccapi_cpp/ccapi_decimal.h"
#include "ccapi_cpp/ccapi_session.h"

namespace ccapi {
AppLogger appLogger;
AppLogger* AppLogger::logger = &appLogger;
CcapiLogger ccapiLogger;
Logger* Logger::logger = &ccapiLogger;

class MyEventHandler : public EventHandler {
 public:
  bool processEvent(const Event& event, Session* session) override {
    auto eventType = event.getType();
    std::vector<Request> requestList;
    if (eventType == Event::Type::SUBSCRIPTION_DATA) {
      for (const auto& message : event.getMessageList()) {
        if (message.getType() == Message::Type::MARKET_DATA_EVENTS_MARKET_DEPTH && message.getRecapType() == Message::RecapType::NONE) {
          auto correlationId = message.getCorrelationIdList().at(0);
          if (correlationId == "s") {
            for (const auto& element : message.getElementList()) {
              if (element.has("BID_PRICE")) {
                bestBidPriceSrc = element.getValue("BID_PRICE");
              }
              if (element.has("ASK_PRICE")) {
                bestAskPriceSrc = element.getValue("ASK_PRICE");
              }
            }
            srcDataReceived = true;
          } else if (correlationId == "d") {
            for (const auto& element : message.getElementList()) {
              if (element.has("BID_PRICE")) {
                bestBidPriceDst = element.getValue("BID_PRICE");
              }
              if (element.has("ASK_PRICE")) {
                bestAskPriceDst = element.getValue("ASK_PRICE");
              }
            }
            dstDataReceived = true;
          }

          if (srcDataReceived && dstDataReceived) {
            double spread1 = (std::stod(bestBidPriceDst) - std::stod(bestBidPriceSrc)) / std::stod(bestBidPriceDst);
            double spread2 = (std::stod(bestAskPriceSrc) - std::stod(bestAskPriceDst)) / std::stod(bestAskPriceSrc);
            bool opportunityExists1 = spread1 > minProfitMargin;
            bool opportunityExists2 = spread2 > minProfitMargin;
            if (makerOrderPlaced) {
              APP_LOGGER_INFO("spread1: " + std::to_string(spread1) + " spread2: " + std::to_string(spread2));
            }
            if (opportunityExists1 && !makerOrderPlaced) {
              APP_LOGGER_INFO("opportunityExists1: " + std::to_string(spread1));
              Request request(Request::Operation::CREATE_ORDER, srcExchange, instrumentRest, "CREATE_ORDER_BUY");
              request.appendParam({
                  {CCAPI_EM_ORDER_SIDE, "BUY"},
                  {CCAPI_EM_ORDER_QUANTITY, orderQuantity},
                  {CCAPI_EM_ORDER_LIMIT_PRICE, bestBidPriceSrc},
                  {CCAPI_INSTRUMENT_TYPE, "linear"},
                  {"timeInForce", "PostOnly"},
              });
              requestList.emplace_back(std::move(request));
              makerOrderPlaced = true;
            } else if (opportunityExists2 && !makerOrderPlaced) {
              APP_LOGGER_INFO("opportunityExists2: " + std::to_string(spread2));
              Request request(Request::Operation::CREATE_ORDER, srcExchange, instrumentRest, "CREATE_ORDER_SELL");
              request.appendParam({
                  {CCAPI_EM_ORDER_SIDE, "SELL"},
                  {CCAPI_EM_ORDER_QUANTITY, orderQuantity},
                  {CCAPI_EM_ORDER_LIMIT_PRICE, bestAskPriceSrc},
                  {CCAPI_INSTRUMENT_TYPE, "linear"},
                  {"timeInForce", "PostOnly"},
              });
              requestList.emplace_back(std::move(request));
              makerOrderPlaced = true;
            } else if ((!opportunityExists1 && !opportunityExists2) && makerOrderPlaced) {
              if (makerOrderId == "") {
                APP_LOGGER_INFO("opportunity no longer exists, canceling order but the order is on the fly, will cancel later");
                makerOrderCancelOnArrival = true;
              } else {
                APP_LOGGER_INFO("opportunity no longer exists, canceling order");
                Request request(Request::Operation::CANCEL_ORDER, srcExchange, instrumentRest);
                request.appendParam({
                    {CCAPI_EM_ORDER_ID, makerOrderId},
                    {CCAPI_INSTRUMENT_TYPE, "linear"},
                });
                requestList.emplace_back(std::move(request));
                makerOrderPlaced = false;
                makerOrderFilled = false;
                makerOrderId = "";
              }
            }
          }
        } else if (message.getType() == Message::Type::EXECUTION_MANAGEMENT_EVENTS_ORDER_UPDATE) {
          for (const auto& element : message.getElementList()) {
            auto quantity = element.getValue(CCAPI_EM_ORDER_QUANTITY);
            auto cumulativeFilledQuantity = element.getValue(CCAPI_EM_ORDER_CUMULATIVE_FILLED_QUANTITY);
            auto remainingQuantity = element.getValue(CCAPI_EM_ORDER_REMAINING_QUANTITY);
            bool filled = false;
            if (!quantity.empty() && !cumulativeFilledQuantity.empty()) {
              filled = Decimal(quantity).toString() == Decimal(cumulativeFilledQuantity).toString();
            } else if (!remainingQuantity.empty()) {
              filled = Decimal(remainingQuantity).toString() == "0";
            }
            if (filled) {
              makerOrderFilled = true;
              std::string side = element.getValue(CCAPI_EM_ORDER_SIDE);
              std::string takerSide;
              if (side == CCAPI_EM_ORDER_SIDE_BUY) {
                takerSide = CCAPI_EM_ORDER_SIDE_SELL;
              } else {
                takerSide = CCAPI_EM_ORDER_SIDE_BUY;
              }

              APP_LOGGER_INFO(element.toString());
              APP_LOGGER_INFO("Maker order filled, placing taker order");
              Request request(Request::Operation::CREATE_ORDER, dstExchange, instrumentRest);
              request.appendParam({
                  {CCAPI_EM_ORDER_SIDE, takerSide},
                  {CCAPI_EM_ORDER_QUANTITY, quantity},
                  {"type", "MARKET"},
              });
              requestList.emplace_back(std::move(request));
              makerOrderPlaced = false;
              makerOrderFilled = false;
              makerOrderId = "";
            }
          }
        }
      }
    } else if (eventType == Event::Type::RESPONSE) {
      const auto& firstMessage = event.getMessageList().at(0);
      const auto& correlationIdList = firstMessage.getCorrelationIdList();
      const auto& messageTimeReceived = firstMessage.getTimeReceived();
      const auto& messageTimeReceivedISO = UtilTime::getISOTimestamp(messageTimeReceived);
      if (firstMessage.getType() == Message::Type::RESPONSE_ERROR) {
        APP_LOGGER_ERROR(event.toStringPretty() + ".");
      }
      if (std::find(correlationIdList.begin(), correlationIdList.end(), std::string("CREATE_ORDER_") + CCAPI_EM_ORDER_SIDE_BUY) != correlationIdList.end() ||
          std::find(correlationIdList.begin(), correlationIdList.end(), std::string("CREATE_ORDER_") + CCAPI_EM_ORDER_SIDE_SELL) != correlationIdList.end() ||
          (std::find(correlationIdList.begin(), correlationIdList.end(), PRIVATE_SUBSCRIPTION_DATA_CORRELATION_ID) != correlationIdList.end() &&
           firstMessage.getType() == Message::Type::CREATE_ORDER)) {
        const auto& element = firstMessage.getElementList().at(0);
        makerOrderId = element.getValue(CCAPI_EM_ORDER_ID);
        if (makerOrderCancelOnArrival) {
          Request request(Request::Operation::CANCEL_ORDER, srcExchange, instrumentRest);
          request.appendParam({
              {CCAPI_EM_ORDER_ID, makerOrderId},
              {CCAPI_INSTRUMENT_TYPE, "linear"},
          });
          requestList.emplace_back(std::move(request));
          makerOrderPlaced = false;
          makerOrderFilled = false;
          makerOrderCancelOnArrival = false;
          makerOrderId = "";
        }
      }
      for (const auto& message : event.getMessageList()) {
        // for (const auto& element : message.getElementList()) {
        //   APP_LOGGER_INFO(element.toString());
        // }
        //   if (message.getType() == Message::Type::EXECUTION_REPORT) {
        //     for (const auto& element : message.getElementList()) {
        //       if (element.has("ORDER_STATUS")) {
        //         std::string orderStatus = element.getValue("ORDER_STATUS");
        //         if (orderStatus == "FILLED") {
        //           makerOrderFilled = true;
        //           filledQuantity = element.getValue("FILLED_QUANTITY");
        //         } else if (orderStatus == "CANCELED") {
        //           makerOrderFilled = false;
        //           makerOrderPlaced = false;
        //         }
        //       }

        //       if (element.has(CCAPI_EM_ORDER_ID)) {
        //         makerOrderId = element.getValue(CCAPI_EM_ORDER_ID);
        //       }
        //     }

        //     if (makerOrderFilled) {
        //       Request requestTaker(Request::Operation::CREATE_ORDER, dstExchange, instrumentRest);
        //       requestTaker.appendParam({
        //           {"SIDE", "SELL"},
        //           {"QUANTITY", filledQuantity},
        //           {"LIMIT_PRICE", bestBidPriceDst},
        //       });
        //       session->sendRequest(requestTaker);
        //     }
        //   }
      }
    } else if (eventType == Event::Type::SUBSCRIPTION_STATUS) {
      for (const auto& message : event.getMessageList()) {
        if (message.getType() == Message::Type::SUBSCRIPTION_STARTED) {
          for (const auto& element : message.getElementList()) {
            APP_LOGGER_INFO(element.getValue(CCAPI_INFO_MESSAGE));
          }
        } else if (message.getType() == Message::Type::SUBSCRIPTION_FAILURE) {
          for (const auto& element : message.getElementList()) {
            APP_LOGGER_ERROR(element.getValue(CCAPI_ERROR_MESSAGE));
          }
        }
      }
    } else if (eventType == Event::Type::SESSION_STATUS) {
      // for (const auto& message : event.getMessageList()) {
      //   if (message.getType() == Message::Type::SESSION_CONNECTION_UP) {
      //     for (const auto& correlationId : message.getCorrelationIdList()) {
      //       if (correlationId == PRIVATE_SUBSCRIPTION_DATA_CORRELATION_ID) {
      //         const auto& messageTimeReceived = message.getTimeReceived();
      //         const auto& messageTimeReceivedISO = UtilTime::getISOTimestamp(messageTimeReceived);
      //         Request request(Request::Operation::CANCEL_ORDER, srcExchange, instrumentRest);
      //         request.appendParam({{CCAPI_EM_ORDER_ID, makerOrderId}});
      //         requestList.emplace_back(std::move(request));
      //         makerOrderPlaced = false;
      //       }
      //     }
      //   }
      // }
    }

    if (!requestList.empty()) {
      if (this->useWebsocketToExecuteOrder) {
        for (auto& request : requestList) {
          auto operation = request.getOperation();
          if (operation == Request::Operation::CREATE_ORDER || operation == Request::Operation::CANCEL_ORDER) {
            request.setCorrelationId(PRIVATE_SUBSCRIPTION_DATA_CORRELATION_ID);
            // session->sendRequestByWebsocket(request);
          } else {
            // session->sendRequest(request);
          }
        }
      } else {
        // for (auto& request : requestList) {
        //   APP_LOGGER_INFO("Sending requests " + request.toString());
        // }
        session->sendRequest(requestList);
      }
    }
    return true;
  }
  std::string srcExchange;
  std::string dstExchange;
  std::string instrumentRest;
  std::string orderQuantity;
  double minProfitMargin{};
  bool useWebsocketToExecuteOrder{};

 private:
  std::string bestBidPriceSrc;
  std::string bestAskPriceSrc;
  std::string bestBidPriceDst;
  std::string bestAskPriceDst;
  bool opportunityExists = false;
  bool makerOrderPlaced = false;
  bool makerOrderFilled = false;
  bool makerOrderCancelOnArrival = false;
  std::string makerOrderId;
  std::string filledQuantity;
  bool srcDataReceived = false;
  bool dstDataReceived = false;
};

} /* namespace ccapi */
using ::ccapi::AppLogger;
using ::ccapi::CcapiLogger;
using ::ccapi::Logger;
using ::ccapi::Message;
using ::ccapi::MyEventHandler;
using ::ccapi::Request;
using ::ccapi::Session;
using ::ccapi::SessionConfigs;
using ::ccapi::SessionOptions;
using ::ccapi::Subscription;
using ::ccapi::UtilString;
using ::ccapi::UtilSystem;
using ::ccapi::UtilTime;

int main(int argc, char** argv) {
  auto now = UtilTime::now();
  std::string srcExchange = UtilSystem::getEnvAsString("SRC_EXCHANGE");
  std::string dstExchange = UtilSystem::getEnvAsString("DST_EXCHANGE");
  std::string instrumentRest = UtilSystem::getEnvAsString("INSTRUMENT");
  std::string instrumentWebsocket = instrumentRest;

  MyEventHandler eventHandler;
  eventHandler.srcExchange = srcExchange;
  eventHandler.dstExchange = dstExchange;
  eventHandler.instrumentRest = instrumentRest;
  eventHandler.minProfitMargin = UtilSystem::getEnvAsDouble("MIN_PROFIT_MARGIN");
  eventHandler.orderQuantity = UtilSystem::getEnvAsString("ORDER_QUANTITY");

  SessionOptions sessionOptions;
  SessionConfigs sessionConfigs;

  Session session(sessionOptions, sessionConfigs, &eventHandler);
  std::vector<Subscription> subscriptionList;
  subscriptionList.emplace_back(srcExchange, instrumentWebsocket, "MARKET_DEPTH", "MARKET_DEPTH_MAX=1", "s"); /* s for src*/
  subscriptionList.emplace_back(dstExchange, instrumentWebsocket, "MARKET_DEPTH", "MARKET_DEPTH_MAX=1", "d"); /* d for dst*/
  subscriptionList.emplace_back(srcExchange, instrumentWebsocket, std::string(CCAPI_EM_PRIVATE_TRADE) + "," + CCAPI_EM_ORDER_UPDATE, "",
                                PRIVATE_SUBSCRIPTION_DATA_CORRELATION_ID);
  for (auto& subscription : subscriptionList) {
    if (subscription.getExchange() == "bybit-derivatives") {
      subscription.setInstrumentType("usdt-contract");
      break;  // assuming there is only one subscription that satisfies the condition
    }
  }

  session.subscribe(subscriptionList);
  while (true) {
    // std::cout << "Monitoring atomic arbitrage opportunities" << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  session.stop();
  return EXIT_SUCCESS;
}
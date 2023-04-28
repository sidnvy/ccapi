#include <sys/stat.h>

#include "ccapi_cpp/ccapi_decimal.h"
#include "app/common.h"
#include "app/event_handler_base.h"
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
          APP_LOGGER_INFO("spread1: " + std::to_string(spread1) + " spread2: " + std::to_string(spread2));
          if (opportunityExists1 && !makerOrderPlaced) {
            APP_LOGGER_INFO("opportunityExists1: " + std::to_string(spread1));
            Request request(Request::Operation::CREATE_ORDER, srcExchange, instrumentRest);
            request.appendParam({
                {"SIDE", "BUY"},
                {"QUANTITY", orderQuantity},
                {"LIMIT_PRICE", bestBidPriceDst},
            });
            requestList.emplace_back(std::move(request));
            makerOrderPlaced = true;
          } else if (opportunityExists2 && !makerOrderPlaced) {
            APP_LOGGER_INFO("opportunityExists2: " + std::to_string(spread2));
            Request request(Request::Operation::CREATE_ORDER, srcExchange, instrumentRest);
            request.appendParam({
                {"SIDE", "SELL"},
                {"QUANTITY", orderQuantity},
                {"LIMIT_PRICE", bestAskPriceSrc},
            });
            requestList.emplace_back(std::move(request));
            makerOrderPlaced = true;
          } else if ((!opportunityExists1 && !opportunityExists2) && makerOrderPlaced) {
            APP_LOGGER_INFO("opportunity no longer exists, canceling order");
            Request request(Request::Operation::CANCEL_ORDER, srcExchange, instrumentRest);
            request.appendParam({{"ORDER_ID", makerOrderId}});
            requestList.emplace_back(std::move(request));
            makerOrderPlaced = false;
          }
        }
      }
    } else if (eventType == Event::Type::RESPONSE) {
      // for (const auto& message : event.getMessageList()) {
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

      //       if (element.has("ORDER_ID")) {
      //         makerOrderId = element.getValue("ORDER_ID");
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
      // }
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
      for (const auto& message : event.getMessageList()) {
        if (message.getType() == Message::Type::SESSION_CONNECTION_UP) {
          for (const auto& correlationId : message.getCorrelationIdList()) {
            if (correlationId == PRIVATE_SUBSCRIPTION_DATA_CORRELATION_ID) {
              const auto& messageTimeReceived = message.getTimeReceived();
              const auto& messageTimeReceivedISO = UtilTime::getISOTimestamp(messageTimeReceived);
              // this->cancelOpenOrders(event, session, requestList, messageTimeReceived, messageTimeReceivedISO, true);
            }
          }
        }
      }
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
        // session->sendRequest(requestList);
      }
    }
    return true;
  }
  std::string srcExchange;
  std::string dstExchange;
  double minProfitMargin{};
  std::string instrumentRest;
  bool useWebsocketToExecuteOrder{};

 private:
  std::string bestBidPriceSrc;
  std::string bestAskPriceSrc;
  std::string bestBidPriceDst;
  std::string bestAskPriceDst;
  std::string orderQuantity = "1";  // You can adjust this value based on the desired order quantity
  bool opportunityExists = false;
  bool makerOrderPlaced = false;
  bool makerOrderFilled = false;
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

  SessionOptions sessionOptions;
  SessionConfigs sessionConfigs;

  Session session(sessionOptions, sessionConfigs, &eventHandler);
  std::vector<Subscription> subscriptionList;
  subscriptionList.emplace_back(srcExchange, instrumentWebsocket, "MARKET_DEPTH", "MARKET_DEPTH_MAX=1", "s"); /* s for src*/
  subscriptionList.emplace_back(dstExchange, instrumentWebsocket, "MARKET_DEPTH", "MARKET_DEPTH_MAX=1", "d"); /* d for dst*/
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
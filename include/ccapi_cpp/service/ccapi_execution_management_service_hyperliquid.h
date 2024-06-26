#ifndef INCLUDE_CCAPI_CPP_SERVICE_CCAPI_EXECUTION_MANAGEMENT_SERVICE_HYPERLIQUID_H_
#define INCLUDE_CCAPI_CPP_SERVICE_CCAPI_EXECUTION_MANAGEMENT_SERVICE_HYPERLIQUID_H_
#ifdef CCAPI_ENABLE_SERVICE_EXECUTION_MANAGEMENT
#ifdef CCAPI_ENABLE_EXCHANGE_HYPERLIQUID
#include "ccapi_cpp/service/ccapi_execution_management_service.h"
#include <ethc/keccak256.h>
#include <ethc/hex.h>
#include <ethc/ecdsa.h>
#include <msgpack.hpp>
#include <gmp.h>
namespace ccapi {
class ExecutionManagementServiceHyperliquid : public ExecutionManagementService {
 public:
  ExecutionManagementServiceHyperliquid(std::function<void(Event& event, Queue<Event>* eventQueue)> eventHandler, SessionOptions sessionOptions, SessionConfigs sessionConfigs, ServiceContextPtr serviceContextPtr): 
    ExecutionManagementService(eventHandler, sessionOptions, sessionConfigs, serviceContextPtr) {
    this->exchangeName = CCAPI_EXCHANGE_NAME_HYPERLIQUID;
    this->baseUrlWs = sessionConfigs.getUrlWebsocketBase().at(this->exchangeName) + "/ws";
    this->baseUrlRest = sessionConfigs.getUrlRestBase().at(this->exchangeName);
    this->setHostRestFromUrlRest(this->baseUrlRest);
    this->setHostWsFromUrlWs(this->baseUrlWs);
    this->apiWalletAddressName = CCAPI_HYPERLIQUID_API_WALLET_ADDRESS;
    this->apiPrivateKeyName = CCAPI_HYPERLIQUID_API_PRIVATE_KEY;
    this->setupCredential({this->apiWalletAddressName, this->apiPrivateKeyName});
    this->createOrderTarget = "/exchange";
    this->cancelOrderTarget = "/exchange";
    this->getOrderTarget = "/info";
    this->getOpenOrdersTarget = "/info";
    this->cancelOpenOrdersTarget = "/exchange";
    this->getAccountBalancesTarget = "/info";
  }

  virtual ~ExecutionManagementServiceHyperliquid() {}
#ifndef CCAPI_EXPOSE_INTERNAL

 private:
#endif
 
#ifdef CCAPI_LEGACY_USE_WEBSOCKETPP
  void pingOnApplicationLevel(wspp::connection_hdl hdl, ErrorCode& ec) override {
    auto now = UtilTime::now();
    auto payload = "{\"method\":\"ping\",\"params\":[],\"time\":" + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count()) + "}";
    this->send(hdl, payload, wspp::frame::opcode::text, ec);
  }
#else
  void pingOnApplicationLevel(std::shared_ptr<WsConnection> wsConnectionPtr, ErrorCode& ec) override {
    auto now = UtilTime::now();
    auto payload = "{\"method\":\"ping\",\"params\":[],\"time\":" + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count()) + "}";
    this->send(wsConnectionPtr, payload, ec);
  }
#endif

  bool doesHttpBodyContainError(const std::string& body) override {
    // return !std::regex_search(body, std::regex("\"status\":\\s*\"ok\""));
    return body.find("error") != std::string::npos;
  }

  void signReqeustForRestGenericPrivateRequest(http::request<http::string_body>& req, const Request& request, std::string& methodString,
                                               std::string& headerString, std::string& path, std::string& queryString, std::string& body, const TimePoint& now,
                                               const std::map<std::string, std::string>& credential) override {
    // Not implemented for Hyperliquid
  }

  void signRequest(http::request<http::string_body>& req, rj::Document& document, const std::map<std::string, std::string>& credential) {
    auto privateKey = mapGetWithDefault(credential, this->apiPrivateKeyName);
    auto nonce = this->generateNonce(UtilTime::now());

    rj::Document::AllocatorType& allocator = document.GetAllocator();
    document.AddMember("nonce", nonce, allocator);

    if (document.HasMember("vaultAddress")) {
      document.AddMember("vaultAddress", rj::Value(document["vaultAddress"].GetString(), allocator).Move(), allocator);
    }

    auto signatureMap = this->signMessage(privateKey, document["action"], nonce);

    rj::Value signature(rj::kObjectType);
    signature.AddMember("r", rj::Value(signatureMap["r"].c_str(), allocator).Move(), allocator);
    signature.AddMember("s", rj::Value(signatureMap["s"].c_str(), allocator).Move(), allocator);
    signature.AddMember("v", std::stoi(signatureMap["v"]), allocator);

    document.AddMember("signature", signature, allocator);
    
    rj::StringBuffer stringBuffer;
    rj::Writer<rj::StringBuffer> writer(stringBuffer);
    document.Accept(writer);
    std::string body = stringBuffer.GetString();

    req.body() = body;
    req.prepare_payload();
  }

  void appendParam(rj::Value& rjValue, rj::Document::AllocatorType& allocator, const std::map<std::string, std::string>& param,
                   const std::map<std::string, std::string> standardizationMap = {
                    {CCAPI_EM_ORDER_SIDE, "b"}, 
                    {CCAPI_EM_ORDER_QUANTITY, "s"},
                    {CCAPI_EM_ORDER_LIMIT_PRICE, "p"},
                    {CCAPI_EM_CLIENT_ORDER_ID, "c"}
                   }) {
    for (const auto& kv : param) {
      auto key = standardizationMap.find(kv.first) != standardizationMap.end() ? standardizationMap.at(kv.first) : kv.first;
      auto value = kv.second;
      if (key == "b") {
        bool isBuy = (value == CCAPI_EM_ORDER_SIDE_BUY || value == "buy");
        rjValue.AddMember(rj::Value(key.c_str(), allocator).Move(), isBuy, allocator);
      } else {
        rjValue.AddMember(rj::Value(key.c_str(), allocator).Move(), rj::Value(value.c_str(), allocator).Move(), allocator);
      }
    }
  }

  void convertRequestForRest(http::request<http::string_body>& req, const Request& request, const TimePoint& now, const std::string& symbolId, const std::map<std::string, std::string>& credential) override {
    switch (request.getOperation()) {
      case Request::Operation::CREATE_ORDER: {
        req.method(http::verb::post);
        req.target(this->createOrderTarget);
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        req.set(beast::http::field::content_type, "application/json");
        
        rj::Document document;
        document.SetObject();
        rj::Document::AllocatorType& allocator = document.GetAllocator();
        
        rj::Value action(rj::kObjectType);
        rj::Value orders(rj::kArrayType);
        rj::Value order(rj::kObjectType);
        action.AddMember("type", rj::Value("order").Move(), allocator);
        order.AddMember("a", std::stoi(symbolId), allocator);
        this->appendParam(order, allocator, param);

        // Add the 't' field for limit orders
        rj::Value tValue(rj::kObjectType);
        rj::Value limitValue(rj::kObjectType);
        // Default to GTC if not specified
        std::string tif = param.find("timeInForce") != param.end() ? param.at("timeInForce") : "Gtc";
        // Default to not reduceOnly if not specified
        std::string reduceOnly = param.find("reduceOnly") != param.end() ? param.at("reduceOnly") :  "false";
        order.AddMember("r", reduceOnly == "true", allocator);
        
        limitValue.AddMember("tif", rj::Value(tif.c_str(), allocator).Move(), allocator);
        tValue.AddMember("limit", limitValue, allocator);
        order.AddMember("t", tValue, allocator);

        rj::Value sortedOrder(rj::kObjectType);
        const char* orderKeys[] = {"a", "b", "p", "s", "r", "t", "c"};
        for (const char* key : orderKeys) {
          if (order.HasMember(key)) {
            sortedOrder.AddMember(rj::Value(key, allocator).Move(), order[key], allocator);
          }
        }
        
        orders.PushBack(sortedOrder, allocator);
        action.AddMember("orders", orders, allocator);
        action.AddMember("grouping", rj::Value("na").Move(), allocator);
        document.AddMember("action", action, allocator);
        
        this->signRequest(req, document, credential);
      } break;
      case Request::Operation::CANCEL_ORDER: {
        req.method(http::verb::post);
        req.target(this->cancelOrderTarget);
        req.set(beast::http::field::content_type, "application/json");
        
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        
        rj::Document document;
        document.SetObject();
        rj::Document::AllocatorType& allocator = document.GetAllocator();
        rj::Value action(rj::kObjectType);
        rj::Value cancels(rj::kArrayType);
        rj::Value cancel(rj::kObjectType);
        if (param.find(CCAPI_EM_CLIENT_ORDER_ID) != param.end()) {
          action.AddMember("type", rj::Value("cancelByCloid").Move(), allocator);
          cancel.AddMember("asset", std::stoi(symbolId), allocator);
          cancel.AddMember("cloid", rj::Value(param.at(CCAPI_EM_CLIENT_ORDER_ID).c_str(), allocator).Move(), allocator);
        } else {
          action.AddMember("type", rj::Value("cancel").Move(), allocator);
          cancel.AddMember("a", std::stoi(symbolId), allocator);
          cancel.AddMember("o", std::stoll(param.at(CCAPI_EM_ORDER_ID)), allocator);
        }
        cancels.PushBack(cancel, allocator);
        action.AddMember("cancels", cancels, allocator);
        document.AddMember("action", action, allocator);
        
        this->signRequest(req, document, credential);
      } break;
      case Request::Operation::GET_ORDER: {
        req.method(http::verb::post);
        req.target(this->getOrderTarget);
        req.set(beast::http::field::content_type, "application/json");
        
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        
        rj::Document document;
        document.SetObject();
        rj::Document::AllocatorType& allocator = document.GetAllocator();
        
        document.AddMember("type", rj::Value("orderStatus").Move(), allocator);
        document.AddMember("user", rj::Value(credential.at(this->apiWalletAddressName).c_str(), allocator).Move(), allocator);
        if (param.find(CCAPI_EM_CLIENT_ORDER_ID) != param.end()) {
          document.AddMember("oid", rj::Value(param.at(CCAPI_EM_CLIENT_ORDER_ID).c_str(), allocator).Move(), allocator);
        } else {
          document.AddMember("oid", std::stoll(param.at(CCAPI_EM_ORDER_ID)), allocator);
        }

        rj::StringBuffer stringBuffer;
        rj::Writer<rj::StringBuffer> writer(stringBuffer);
        document.Accept(writer);
        std::string body = stringBuffer.GetString();
        req.body() = body;
        req.prepare_payload();
      } break;
      case Request::Operation::GET_OPEN_ORDERS: {
        req.method(http::verb::post);
        req.target(this->getOpenOrdersTarget);
        req.set(beast::http::field::content_type, "application/json");
        
        rj::Document document;
        document.SetObject();
        rj::Document::AllocatorType& allocator = document.GetAllocator();
        
        document.AddMember("type", rj::Value("openOrders").Move(), allocator);
        document.AddMember("user", rj::Value(credential.at(this->apiWalletAddressName).c_str(), allocator).Move(), allocator);
        
        rj::StringBuffer stringBuffer;
        rj::Writer<rj::StringBuffer> writer(stringBuffer);
        document.Accept(writer);
        std::string body = stringBuffer.GetString();
        req.body() = body;
        req.prepare_payload();
      } break;
      case Request::Operation::GET_ACCOUNT_POSITIONS:
      case Request::Operation::GET_ACCOUNT_BALANCES: {
        req.method(http::verb::post);
        req.target(this->getAccountBalancesTarget);
        req.set(beast::http::field::content_type, "application/json");
        
        rj::Document document;
        document.SetObject();
        rj::Document::AllocatorType& allocator = document.GetAllocator();
        
        document.AddMember("type", rj::Value("clearinghouseState").Move(), allocator);
        document.AddMember("user", rj::Value(credential.at(this->apiWalletAddressName).c_str(), allocator).Move(), allocator);
        
        rj::StringBuffer stringBuffer;
        rj::Writer<rj::StringBuffer> writer(stringBuffer);
        document.Accept(writer);
        std::string body = stringBuffer.GetString();
        req.body() = body;
        req.prepare_payload();
      } break;
      default:
        this->convertRequestForRestCustom(req, request, now, symbolId, credential);
    }
  }

  void extractOrderInfoFromRequest(std::vector<Element>& elementList, const Request& request, const Request::Operation operation,
                                   const rj::Document& document) override {
    switch (operation) {
      case Request::Operation::CREATE_ORDER:
      case Request::Operation::CANCEL_ORDER: {
        this->extractOrderInfoFromCreateOrCancelOrderRequest(elementList, document);
      } break;
      case Request::Operation::GET_ORDER:
      case Request::Operation::GET_OPEN_ORDERS: {
        this->extractOrderInfoFromGetOpenOrdersRequest(elementList, document);
      } break;
      default:
        break;
    }
  }

  void extractOrderInfoFromCreateOrCancelOrderRequest(std::vector<Element>& elementList, const rj::Document& document) {
    const std::map<std::string, std::pair<std::string, JsonDataType>>& extractionFieldNameMap = {
        {CCAPI_EM_ORDER_ID, std::make_pair("oid", JsonDataType::INTEGER)},
        {CCAPI_EM_CLIENT_ORDER_ID, std::make_pair("cloid", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_SIDE, std::make_pair("side", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_QUANTITY, std::make_pair("origSz", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_LIMIT_PRICE, std::make_pair("limitPx", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_CUMULATIVE_FILLED_QUANTITY, std::make_pair("totalSz", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_AVERAGE_FILLED_PRICE, std::make_pair("avgPx", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_INSTRUMENT, std::make_pair("asset", JsonDataType::STRING)}};
        // {CCAPI_EM_ORDER_STATUS, std::make_pair("state", JsonDataType::STRING)},
    
    const rj::Value& response = document["response"];
    const std::string& type = response["type"].GetString();
    if (type == "order") {
      const rj::Value& statuses = response["data"]["statuses"];
      
      for (const auto& status : statuses.GetArray()) {
        Element element;
        if (status.HasMember("resting")) {
          const rj::Value& resting = status["resting"];
          this->extractOrderInfo(element, resting, extractionFieldNameMap);
          element.insert(CCAPI_EM_ORDER_STATUS, "resting");
        } else if (status.HasMember("filled")) {
          const rj::Value& filled = status["filled"];
          this->extractOrderInfo(element, filled, extractionFieldNameMap);
          element.insert(CCAPI_EM_ORDER_STATUS, "filled");
        } else if (status.HasMember("error")) {
          element.insert(CCAPI_EM_ORDER_STATUS, "error");
          element.insert(CCAPI_ERROR_MESSAGE, status["error"].GetString());
        }
        elementList.emplace_back(std::move(element));
      }
    } else {
      // Handle other response types if necessary
    }  
  }

  void extractOrderInfoFromGetOpenOrdersRequest(std::vector<Element>& elementList, const rj::Document& document) {
    const std::map<std::string, std::pair<std::string, JsonDataType>>& extractionFieldNameMap = {
        {CCAPI_EM_ORDER_ID, std::make_pair("oid", JsonDataType::INTEGER)},
        {CCAPI_EM_CLIENT_ORDER_ID, std::make_pair("cloid", JsonDataType::STRING)},
        // {CCAPI_EM_ORDER_SIDE, std::make_pair("side", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_QUANTITY, std::make_pair("origSz", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_LIMIT_PRICE, std::make_pair("limitPx", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_CUMULATIVE_FILLED_QUANTITY, std::make_pair("totalSz", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_AVERAGE_FILLED_PRICE, std::make_pair("avgPx", JsonDataType::STRING)},
        {CCAPI_EM_ORDER_INSTRUMENT, std::make_pair("coin", JsonDataType::STRING)}};

    for (const auto& order : document.GetArray()) {
      Element element;
      this->extractOrderInfo(element, order, extractionFieldNameMap);
      const char* sideStr = order["side"].GetString();
      bool isBuy = (std::strcmp(sideStr, "B") == 0);
      element.insert(CCAPI_EM_ORDER_SIDE, isBuy ? CCAPI_EM_ORDER_SIDE_BUY : CCAPI_EM_ORDER_SIDE_SELL);
      elementList.emplace_back(std::move(element));
    }
  }

  void extractAccountInfoFromRequest(std::vector<Element>& elementList, const Request& request, const Request::Operation operation,
                                     const rj::Document& document) override {
    switch (operation) {
      case Request::Operation::GET_ACCOUNT_BALANCES: {
        if (document.HasMember("marginSummary")) {
          const rj::Value& marginSummary = document["marginSummary"];
          Element element;
          element.insert(CCAPI_EM_ASSET, "USDC");
          element.insert(CCAPI_EM_QUANTITY_AVAILABLE_FOR_TRADING, marginSummary["accountValue"].GetString());
          element.insert(CCAPI_EM_QUANTITY_TOTAL, marginSummary["totalRawUsd"].GetString());
          elementList.emplace_back(std::move(element));
        }
      } break;
      case Request::Operation::GET_ACCOUNT_POSITIONS: {
        if (document.HasMember("assetPositions")) {
          const rj::Value& assetPositions = document["assetPositions"];
          for (const auto& position : assetPositions.GetArray()) {
            Element element;
            // element.insert(CCAPI_EM_SYMBOL, position["position"]["coin"].GetString() + "/USDC");
            // element.insert(CCAPI_EM_POSITION_SIDE, position["position"]["szi"].GetDouble() > 0 ? CCAPI_EM_POSITION_SIDE_LONG : CCAPI_EM_POSITION_SIDE_SHORT);
            element.insert(CCAPI_EM_POSITION_QUANTITY, std::to_string(std::abs(position["position"]["szi"].GetDouble())));
            element.insert(CCAPI_EM_POSITION_COST, std::to_string(position["position"]["positionValue"].GetDouble()));
            element.insert(CCAPI_EM_POSITION_LEVERAGE, std::to_string(position["position"]["leverage"]["value"].GetDouble()));
            elementList.emplace_back(std::move(element));
          }
        }
      } break;
      default:
        break;
    }
  }

  void extractOrderInfo(Element& element, const rj::Value& x, const std::map<std::string, std::pair<std::string, JsonDataType>>& extractionFieldNameMap,
                        const std::map<std::string, std::function<std::string(const std::string&)>> conversionMap = {}) override {
    ExecutionManagementService::extractOrderInfo(element, x, extractionFieldNameMap);
  }

  std::vector<std::string> createSendStringListFromSubscription(const WsConnection& wsConnection, const Subscription& subscription, const TimePoint& now,
                                                                const std::map<std::string, std::string>& credential) override {
    rj::Document document;
    document.SetObject();
    auto& allocator = document.GetAllocator();

    document.AddMember("method", rj::Value("subscribe").Move(), allocator);

    rj::Value subscribe(rj::kObjectType);

    auto fieldSet = subscription.getFieldSet();
    if (fieldSet.find(CCAPI_EM_ORDER_UPDATE) != fieldSet.end()) {
      subscribe.AddMember("type", rj::Value("orderUpdates").Move(), allocator);
    } else if (fieldSet.find(CCAPI_EM_PRIVATE_TRADE) != fieldSet.end()) {
      subscribe.AddMember("type", rj::Value("userFills").Move(), allocator);
    }
    subscribe.AddMember("user", rj::Value(credential.at(this->apiWalletAddressName).c_str(), allocator).Move(), allocator);

    document.AddMember("subscription", subscribe, allocator);

    rj::StringBuffer stringBuffer;
    rj::Writer<rj::StringBuffer> writer(stringBuffer);
    document.Accept(writer);
    std::string sendString = stringBuffer.GetString();

    std::vector<std::string> sendStringList;
    sendStringList.push_back(sendString);
    return sendStringList;
  }

  void onTextMessage(
#ifdef CCAPI_LEGACY_USE_WEBSOCKETPP
      const WsConnection& wsConnection, const Subscription& subscription, const std::string& textMessage
#else
      std::shared_ptr<WsConnection> wsConnectionPtr, const Subscription& subscription, boost::beast::string_view textMessageView
#endif
      ,
      const TimePoint& timeReceived) override {
#ifdef CCAPI_LEGACY_USE_WEBSOCKETPP
#else
    std::string textMessage(textMessageView);
#endif
    rj::Document document;
    document.Parse<rj::kParseNumbersAsStringsFlag>(textMessage.c_str());
    auto channel = std::string(document["channel"].GetString());
    Event event = this->createEvent(subscription, textMessage, document, channel, timeReceived);
    if (!event.getMessageList().empty()) {
      this->eventHandler(event, nullptr);
    }
  }

  Event createEvent(const Subscription& subscription, const std::string& textMessage, const rj::Document& document, const std::string& channel, const TimePoint& timeReceived) {
    Event event;
    std::vector<Message> messageList;

    if (channel == "orderUpdates") {
      const rj::Value& data = document["data"];
      for (const auto& order : data.GetArray()) {
        Message message;
        message.setTimeReceived(timeReceived);
        message.setType(Message::Type::EXECUTION_MANAGEMENT_EVENTS_ORDER_UPDATE);
        message.setCorrelationIdList({subscription.getCorrelationId()});
        std::vector<Element> elementList;
        const std::map<std::string, std::pair<std::string, JsonDataType>>& extractionFieldNameMap = {
            {CCAPI_EM_ORDER_ID, std::make_pair("oid", JsonDataType::INTEGER)},
            {CCAPI_EM_CLIENT_ORDER_ID, std::make_pair("cloid", JsonDataType::STRING)},
            {CCAPI_EM_ORDER_QUANTITY, std::make_pair("origSz", JsonDataType::STRING)},
            {CCAPI_EM_ORDER_LIMIT_PRICE, std::make_pair("limitPx", JsonDataType::STRING)},
            {CCAPI_EM_ORDER_CUMULATIVE_FILLED_QUANTITY, std::make_pair("sz", JsonDataType::STRING)},
            {CCAPI_EM_ORDER_STATUS, std::make_pair("state", JsonDataType::STRING)},
            {CCAPI_EM_ORDER_INSTRUMENT, std::make_pair("coin", JsonDataType::STRING)}};
        Element element;
        this->extractOrderInfo(element, order["order"], extractionFieldNameMap);
        element.insert(CCAPI_EM_ORDER_STATUS, std::string(order["status"].GetString()));
        element.insert(CCAPI_EM_ORDER_SIDE, std::strcmp(order["order"]["side"].GetString(), "B") == 0 ? CCAPI_EM_ORDER_SIDE_BUY : CCAPI_EM_ORDER_SIDE_SELL);
        elementList.emplace_back(std::move(element));
        message.setElementList(elementList);
        messageList.emplace_back(std::move(message));
      }
    } else if (channel == "userFills") {
      const rj::Value& data = document["data"];
      if (!(data.HasMember("isSnapshot") && data["isSnapshot"].GetBool())) {
        for (const auto& fill : data["fills"].GetArray()) {
          Message message;
          message.setTimeReceived(timeReceived);
          message.setType(Message::Type::EXECUTION_MANAGEMENT_EVENTS_PRIVATE_TRADE);
          message.setCorrelationIdList({subscription.getCorrelationId()});
          std::vector<Element> elementList;
          Element element;
          element.insert(CCAPI_TRADE_ID, std::string(fill["tid"].GetString()));
          element.insert(CCAPI_EM_ORDER_LAST_EXECUTED_PRICE, fill["px"].GetString());
          element.insert(CCAPI_EM_ORDER_LAST_EXECUTED_SIZE, fill["sz"].GetString());
          element.insert(CCAPI_EM_ORDER_SIDE, std::strcmp(fill["side"].GetString(), "B") == 0 ? CCAPI_EM_ORDER_SIDE_BUY : CCAPI_EM_ORDER_SIDE_SELL);
          element.insert(CCAPI_EM_POSITION_SIDE, fill["dir"].GetString());
          element.insert(CCAPI_IS_MAKER, fill["crossed"].GetBool() ? "0" : "1");
          element.insert(CCAPI_EM_ORDER_ID, std::string(fill["oid"].GetString()));
          element.insert(CCAPI_EM_ORDER_INSTRUMENT, std::string(fill["coin"].GetString()));
          element.insert(CCAPI_EM_ORDER_FEE_QUANTITY, std::string(fill["fee"].GetString()));
          element.insert(CCAPI_EM_ORDER_FEE_ASSET, std::string(fill["feeToken"].GetString()));
          elementList.emplace_back(std::move(element));
          message.setElementList(elementList);
          messageList.emplace_back(std::move(message));
        }
      }
    } else if (channel == "post") {
      const rj::Value& data = document["data"];
      if (data.IsObject() && data.HasMember("response")) {
        const rj::Value& response = data["response"];
        std::string type = response["type"].GetString();
        if (type == "error") {
          std::string errorMessage = response["payload"].GetString();
          CCAPI_LOGGER_ERROR("Received error message: " + errorMessage);
          Event event;
          event.setType(Event::Type::RESPONSE);
          Message message;
          message.setType(Message::Type::RESPONSE_ERROR);
          message.setTimeReceived(timeReceived);
          message.setCorrelationIdList({subscription.getCorrelationId()});
          Element element;
          element.insert(CCAPI_ERROR_MESSAGE, errorMessage);
          message.setElementList({element});
          messageList.emplace_back(std::move(message));
        }
      }
    }

    event.setType(Event::Type::SUBSCRIPTION_DATA);
    event.addMessages(messageList);
    return event;
  }

  int64_t generateNonce(const TimePoint& now) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
  }

  std::string toHex(const uint8_t* data, size_t len) {
      std::string input(reinterpret_cast<const char*>(data), len);
      std::string hexString = UtilAlgorithm::stringToHex(input);
      return "0x" + hexString;
  }

  void convertToMsgpack(const rj::Value& value, msgpack::packer<msgpack::sbuffer>& packer) {
      switch (value.GetType()) {
          case rj::kNullType: packer.pack_nil(); break;
          case rj::kFalseType: packer.pack_false(); break;
          case rj::kTrueType: packer.pack_true(); break;
          case rj::kObjectType:
              packer.pack_map(value.MemberCount());
              for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it) {
                  packer.pack(it->name.GetString());
                  convertToMsgpack(it->value, packer);
              }
              break;
          case rj::kArrayType:
              packer.pack_array(value.Size());
              for (auto it = value.Begin(); it != value.End(); ++it) {
                  convertToMsgpack(*it, packer);
              }
              break;
          case rj::kStringType: packer.pack(value.GetString()); break;
          case rj::kNumberType:
              if (value.IsInt()) packer.pack(value.GetInt());
              else if (value.IsUint()) packer.pack(value.GetUint());
              else if (value.IsInt64()) packer.pack(value.GetInt64());
              else if (value.IsUint64()) packer.pack(value.GetUint64());
              else if (value.IsDouble()) packer.pack(value.GetDouble());
              break;
      }
  }

  std::vector<uint8_t> calculateConnectionId(const rj::Value& action, uint64_t nonce) {
      msgpack::sbuffer sbuf;
      msgpack::packer<msgpack::sbuffer> packer(&sbuf);
      convertToMsgpack(action, packer);
      std::vector<uint8_t> data(sbuf.data(), sbuf.data() + sbuf.size());
      for (int i = 7; i >= 0; --i) {
          data.push_back((nonce >> (i * 8)) & 0xFF);
      }
      data.push_back(0x00);
      std::vector<uint8_t> connection_id(32);
      eth_keccak256(connection_id.data(), data.data(), data.size());
      return connection_id;
  }

  std::string encodeType(const std::string& primary_type, const std::map<std::string, std::vector<std::map<std::string, std::string>>>& types) {
      std::string encoded = primary_type + "(";
      for (const auto& field : types.at(primary_type)) {
          if (&field != &types.at(primary_type)[0]) encoded += ",";
          encoded += field.at("type") + " " + field.at("name");
      }
      encoded += ")";
      return encoded;
  }

  std::vector<uint8_t> hashType(const std::string& primary_type, const std::map<std::string, std::vector<std::map<std::string, std::string>>>& types) {
      std::string encoded_type = encodeType(primary_type, types);
      std::vector<uint8_t> type_hash(32);
      eth_keccak256(type_hash.data(), (uint8_t*)encoded_type.c_str(), encoded_type.length());
      return type_hash;
  }

  std::vector<uint8_t> encodeData(const std::string& primary_type, 
                                   const std::map<std::string, std::vector<std::map<std::string, std::string>>>& types, 
                                   const std::map<std::string, std::string>& data) {
      std::vector<uint8_t> encoded;
      std::vector<uint8_t> type_hash = hashType(primary_type, types);
      encoded.insert(encoded.end(), type_hash.begin(), type_hash.end());

      for (const auto& field : types.at(primary_type)) {
          std::string name = field.at("name");
          std::string type = field.at("type");
          std::string value = data.at(name);
          std::vector<uint8_t> field_value(32, 0);

          if (type == "string") {
              eth_keccak256(field_value.data(), (uint8_t*)value.c_str(), value.length());
          } else if (type == "uint256") {
              mpz_t mpz;
              mpz_init(mpz);
              mpz_set_str(mpz, value.c_str(), 10);
              size_t count;
              mpz_export(field_value.data() + (32 - mpz_sizeinbase(mpz, 256)), &count, 1, 1, 0, 0, mpz);
              mpz_clear(mpz);
          } else if (type == "address") {
              std::string address = value.substr(0, 2) == "0x" ? value.substr(2) : value;
              for (size_t i = 0; i < 20; ++i) {
                  field_value[12 + i] = std::stoi(address.substr(i * 2, 2), nullptr, 16);
              }
          } else if (type == "bytes32") {
              std::copy(value.begin(), value.end(), field_value.begin());
          }
          encoded.insert(encoded.end(), field_value.begin(), field_value.end());
      }
      return encoded;
  }

  std::vector<uint8_t> hashData(const std::vector<uint8_t>& data) {
      std::vector<uint8_t> hashed(32);
      eth_keccak256(hashed.data(), data.data(), data.size());
      return hashed;
  }

  std::string vectorToPythonByteString(const std::vector<uint8_t>& v) {
      std::stringstream ss;
      ss << "b'";
      for (const auto& byte : v) {
          if (std::isprint(byte) && byte != '\\' && byte != '\'') {
              ss << static_cast<char>(byte);
          } else {
              ss << "\\x" << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(byte);
          }
      }
      ss << "'";
      return ss.str();
  }

  std::map<std::string, std::string> signMessage(const std::string& private_key_hex, const rj::Value& action, uint64_t nonce) {
      if (private_key_hex.empty()) {
        throw std::runtime_error("Private key not exist");
      }

      std::vector<uint8_t> connection_id = calculateConnectionId(action, nonce);
      std::map<std::string, std::string> domain = {
          {"name", "Exchange"},
          {"version", "1"},
          {"chainId", "1337"},
          {"verifyingContract", "0x0000000000000000000000000000000000000000"}
      };

      std::map<std::string, std::vector<std::map<std::string, std::string>>> types = {
          {"EIP712Domain", {
              {{"name", "name"}, {"type", "string"}},
              {{"name", "version"}, {"type", "string"}},
              {{"name", "chainId"}, {"type", "uint256"}},
              {{"name", "verifyingContract"}, {"type", "address"}}
          }},
          {"Agent", {
              {{"name", "source"}, {"type", "string"}},
              {{"name", "connectionId"}, {"type", "bytes32"}}
          }}
      };

      std::map<std::string, std::string> message = {
          {"source", "a"},
          {"connectionId", std::string(connection_id.begin(), connection_id.end())}
      };

      std::vector<uint8_t> domain_separator = hashData(encodeData("EIP712Domain", types, domain));
      std::vector<uint8_t> message_hash = hashData(encodeData("Agent", types, message));

      std::vector<uint8_t> eip191_hash(32);
      std::vector<uint8_t> eip191_prefix = {0x19, 0x01};
      std::vector<uint8_t> eip191_data;
      eip191_data.insert(eip191_data.end(), eip191_prefix.begin(), eip191_prefix.end());
      eip191_data.insert(eip191_data.end(), domain_separator.begin(), domain_separator.end());
      eip191_data.insert(eip191_data.end(), message_hash.begin(), message_hash.end());
      eth_keccak256(eip191_hash.data(), eip191_data.data(), eip191_data.size());

      uint8_t* private_key_bytes = nullptr;
      int bytes_len = eth_hex_to_bytes(&private_key_bytes, private_key_hex.c_str(), -1);
      if (bytes_len != 32) {
          free(private_key_bytes);
          throw std::runtime_error("Invalid private key length");
      }

      struct eth_ecdsa_signature signature;
      int result = eth_ecdsa_sign(&signature, private_key_bytes, eip191_hash.data());
      free(private_key_bytes);

      if (result != 1) {
          throw std::runtime_error("eth_ecdsa_sign failed");
      }

      return {
          {"r", toHex(signature.r, 32)},
          {"s", toHex(signature.s, 32)},
          {"v", std::to_string(signature.recid + 27)}
      };
  }

 private:
  std::string apiWalletAddressName;
  std::string apiPrivateKeyName;
};
} /* namespace ccapi */
#endif
#endif
#endif  // INCLUDE_CCAPI_CPP_SERVICE_CCAPI_EXECUTION_MANAGEMENT_SERVICE_HYPERLIQUID_H_

//+------------------------------------------------------------------+
//|                                                  receiveslave.mq5 |
//|                                  Copyright 2025, MetaQuotes Ltd. |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+
#property copyright "Copyright 2025, MetaQuotes Ltd."
#property link      "https://www.mql5.com"
#property version   "1.00"

#include <Zmq/Zmq.mqh>

input int ServerPort = 5000;  // ZMQ server port
input int SlaveMagicID = 123456;  // Magic ID for slave trades
input int SlippagePoints = 10;  // Maximum slippage in points
input int PollInterval = 1;  // Polling interval in milliseconds (1 = 1ms for fast response)

// ZMQ Context and Socket
Context zmqContext("PositionReceiver");
Socket zmqSocket(zmqContext, ZMQ_REP);
bool zmqBound = false;

// Cache for master ticket -> slave ticket mappings
struct TicketMapping
  {
   ulong masterTicket;
   ulong slaveTicket;
   string symbol;
   datetime openTime;
  };
TicketMapping ticketCache[];  // In-memory cache for ticket mappings

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
  {
//---
   Print("========================================");
   Print(">>> ZMQ POSITION RECEIVER INITIALIZING <<<");
   Print("========================================");
   
   // Set socket options for immediate release on close
   zmqSocket.setLinger(0);  // Don't wait for pending messages on close
   
   // Bind ZMQ socket
   string bindAddress = "tcp://*:" + IntegerToString(ServerPort);
   Print("Binding ZMQ socket to: ", bindAddress);
   
   // Try to bind with a small retry delay in case port is still being released
   int retryCount = 0;
   int maxRetries = 5;
   bool bindSuccess = false;
   
   while(retryCount < maxRetries)
     {
      if(zmqSocket.bind(bindAddress))
        {
         bindSuccess = true;
         break;
        }
      retryCount++;
      if(retryCount < maxRetries)
        {
         Print("WARNING: Failed to bind (attempt ", retryCount, "/", maxRetries, "). Retrying in 100ms...");
         Sleep(100);  // Wait 100ms before retry
        }
     }
   
   if(!bindSuccess)
     {
      Print("ERROR: Failed to bind ZMQ socket to port ", ServerPort, " after ", maxRetries, " attempts");
      Print("  Is another EA already using this port?");
      Print("  Try removing the old EA instance from the chart first.");
      return(INIT_FAILED);
     }
   
   zmqBound = true;
   Print("✓ ZMQ socket bound successfully");
   Print("  Port: ", ServerPort);
   Print("  Magic ID: ", SlaveMagicID);
   Print("  Slippage: ", SlippagePoints, " points");
   
   // Start high-speed timer for non-blocking message polling
   EventSetMillisecondTimer(PollInterval);
   Print("  Timer started - polling every ", PollInterval, " ms");
   Print("========================================");
   Print("  ✓ Ticket cache initialized (in-memory storage)");
   Print("  Cache will store master->slave ticket mappings");
   Print("========================================");
   
//---
   return(INIT_SUCCEEDED);
  }
//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
  {
//---
   EventKillTimer();
   
   // Unbind and close ZMQ socket before deinitialization
   if(zmqBound)
     {
      string bindAddress = "tcp://*:" + IntegerToString(ServerPort);
      Print("Unbinding ZMQ socket from: ", bindAddress);
      zmqSocket.unbind(bindAddress);
      zmqBound = false;
      Print("✓ ZMQ socket unbound successfully");
     }
   
   ArrayFree(ticketCache);
   Print("ZMQ Position Receiver deinitialized.");
//---
  }
//+------------------------------------------------------------------+
//| Expert tick function                                             |
//+------------------------------------------------------------------+
void OnTick()
  {
//---
   // OnTick() not used - we use OnTimer() for message polling
//---
  }
//+------------------------------------------------------------------+
//| Timer function - polls ZMQ socket for new messages              |
//+------------------------------------------------------------------+
void OnTimer()
  {
   if(!zmqBound) return;
   
   // Non-blocking receive (timeout = 0 means don't wait)
   ZmqMsg request;
   if(!zmqSocket.recv(request, 0))  // 0 = non-blocking
      return;  // No message available
   
   // Message received!
   string jsonData = request.getData();
   Print("========================================");
   Print(">>> RECEIVED POSITION DATA VIA ZMQ <<<");
   Print("  JSON: ", jsonData);
   Print("========================================");
   
   // Parse JSON and process position
   ProcessPositionData(jsonData);
  }
//+------------------------------------------------------------------+
//| Parse JSON and extract field value                              |
//+------------------------------------------------------------------+
string ExtractJsonField(string json, string fieldName)
  {
   string searchStr = "\"" + fieldName + "\":";
   int pos = StringFind(json, searchStr);
   if(pos < 0) return "";
   
   // Find the value after the colon
   int startPos = pos + StringLen(searchStr);
   
   // Skip whitespace
   while(startPos < StringLen(json) && StringGetCharacter(json, startPos) == ' ')
      startPos++;
   
   // Check if value is a string (starts with quote)
   if(StringGetCharacter(json, startPos) == '"')
     {
      // Extract string value
      startPos++;  // Skip opening quote
      int endPos = startPos;
      while(endPos < StringLen(json) && StringGetCharacter(json, endPos) != '"')
         endPos++;
      return StringSubstr(json, startPos, endPos - startPos);
     }
   else
     {
      // Extract numeric or boolean value
      int endPos = startPos;
      while(endPos < StringLen(json) && 
            StringGetCharacter(json, endPos) != ',' && 
            StringGetCharacter(json, endPos) != '}' &&
            StringGetCharacter(json, endPos) != ' ')
         endPos++;
      return StringSubstr(json, startPos, endPos - startPos);
     }
  }
//+------------------------------------------------------------------+
//| Process position data from JSON                                 |
//+------------------------------------------------------------------+
void ProcessPositionData(string jsonData)
  {
   // Extract fields from JSON
   string timeStr = ExtractJsonField(jsonData, "Time");
   string ticketStr = ExtractJsonField(jsonData, "Ticket");
   string symbol = ExtractJsonField(jsonData, "Symbol");
   string typeStr = ExtractJsonField(jsonData, "Type");
   string actionStr = ExtractJsonField(jsonData, "Action");
   string volumeStr = ExtractJsonField(jsonData, "Volume");
   string priceStr = ExtractJsonField(jsonData, "Price");
   string slStr = ExtractJsonField(jsonData, "SL");
   string tpStr = ExtractJsonField(jsonData, "TP");
   string comment = ExtractJsonField(jsonData, "Comment");
   string magicIDStr = ExtractJsonField(jsonData, "MagicID");
   
   // Validate required fields
   if(ticketStr == "" || symbol == "" || actionStr == "")
     {
      Print("ERROR: Missing required fields in JSON data");
      SendReply("ERROR|Missing required fields");
      return;
     }
   
   // Parse values
   ulong masterTicket = (ulong)StringToInteger(ticketStr);
   double volume = StringToDouble(volumeStr);
   double price = StringToDouble(priceStr);
   double sl = StringToDouble(slStr);
   double tp = StringToDouble(tpStr);
   
   // Normalize action string
   string actionUpper = actionStr;
   StringToUpper(actionUpper);
   StringTrimLeft(actionUpper);
   StringTrimRight(actionUpper);
   
   // Print received data
   Print("═══════════════════════════════════════════════════════════");
   Print("POSITION DATA RECEIVED:");
   Print("  Time: ", (timeStr != "" ? timeStr : "N/A"));
   Print("  Master Ticket: ", masterTicket);
   Print("  Symbol: ", symbol);
   Print("  Type: ", typeStr);
   Print("  Action: ", actionUpper);
   Print("  Volume: ", DoubleToString(volume, 2), " lots");
   Print("  Price: ", DoubleToString(price, 5));
   Print("  SL: ", (sl > 0 ? DoubleToString(sl, 5) : "Not set"));
   Print("  TP: ", (tp > 0 ? DoubleToString(tp, 5) : "Not set"));
   Print("  Comment: ", (comment != "" ? comment : "No comment"));
   Print("═══════════════════════════════════════════════════════════");
   
   // Process based on action
   if(actionUpper == "OPEN")
     {
      ProcessOpenPosition(masterTicket, symbol, typeStr, volume, price, sl, tp, comment);
     }
   else if(actionUpper == "CLOSE")
     {
      ProcessClosePosition(masterTicket);
     }
   else
     {
      Print("WARNING: Unknown action '", actionUpper, "'. Expected 'OPEN' or 'CLOSE'");
      SendReply("ERROR|Unknown action: " + actionUpper);
     }
  }
//+------------------------------------------------------------------+
//| Process open position                                            |
//+------------------------------------------------------------------+
void ProcessOpenPosition(ulong masterTicket, string symbol, string typeStr, 
                         double volume, double price, double sl, double tp, string comment)
  {
   Print(">>> PROCESSING OPEN POSITION <<<");
   
   // Find best similar symbol on slave broker
   string slaveSymbol = FindBestSimilarSymbol(symbol);
   if(slaveSymbol == "")
     {
      Print("ERROR: Could not find matching symbol for '", symbol, "' on slave broker");
      SendReply("ERROR|Symbol not found: " + symbol);
      return;
     }
   
   // Execute open trade
   ulong slaveTicket = ExecuteOpenTrade(slaveSymbol, typeStr, volume, price, sl, tp, comment);
   
   if(slaveTicket > 0)
     {
      // Store mapping in cache
      StoreTicketMapping(masterTicket, slaveTicket, symbol);
      
      Print("✓ Trade executed - Master ticket: ", masterTicket, " -> Slave ticket: ", slaveTicket);
      Print("  Mapping stored in cache (", ArraySize(ticketCache), " total mappings)");
      
      // Send success reply
      SendReply("SUCCESS|" + IntegerToString(slaveTicket));
     }
   else
     {
      Print("✗ Failed to execute open trade");
      SendReply("ERROR|Trade execution failed");
     }
  }
//+------------------------------------------------------------------+
//| Process close position                                           |
//+------------------------------------------------------------------+
void ProcessClosePosition(ulong masterTicket)
  {
   Print(">>> PROCESSING CLOSE POSITION <<<");
   
   // Get slave ticket from cache
   ulong slaveTicket = GetSlaveTicketFromCache(masterTicket);
   
   if(slaveTicket == 0)
     {
      Print("WARNING: No slave ticket found for master ticket: ", masterTicket);
      Print("  Cache size: ", ArraySize(ticketCache), " mappings");
      SendReply("ERROR|Slave ticket not found for master: " + IntegerToString(masterTicket));
      return;
     }
   
   Print("  Found slave ticket in cache: ", slaveTicket);
   
   // Check if position exists
   if(!PositionSelectByTicket(slaveTicket))
     {
      Print("WARNING: Position with ticket ", slaveTicket, " does not exist");
      Print("  It may have been closed already or never opened");
      Print("  Removing from cache...");
      RemoveTicketMapping(masterTicket);
      SendReply("ERROR|Position not found: " + IntegerToString(slaveTicket));
      return;
     }
   
   // Close the position
   bool closed = ClosePositionByTicket(slaveTicket);
   
   if(closed)
     {
      Print("✓ Position closed successfully");
      // Remove from cache after successful close
      RemoveTicketMapping(masterTicket);
      SendReply("SUCCESS|Position closed: " + IntegerToString(slaveTicket));
     }
   else
     {
      Print("✗ Failed to close position");
      SendReply("ERROR|Close failed");
     }
  }
//+------------------------------------------------------------------+
//| Send reply back to master                                        |
//+------------------------------------------------------------------+
void SendReply(string replyMessage)
  {
   ZmqMsg reply(replyMessage);
   if(zmqSocket.send(reply))
     {
      Print("  ✓ Reply sent: ", replyMessage);
     }
   else
     {
      Print("  ✗ Failed to send reply");
     }
  }
//+------------------------------------------------------------------+
//| Find best similar symbol from slave broker                       |
//+------------------------------------------------------------------+
string FindBestSimilarSymbol(string masterSymbol)
  {
   // Step 1: Try exact match first
   if(SymbolInfoInteger(masterSymbol, SYMBOL_SELECT))
     {
      return masterSymbol;
     }
   
   // Step 2: Get all symbols from slave broker (false = all symbols, not just market watch)
   int totalSymbols = SymbolsTotal(false);
   string bestMatch = "";
   int bestScore = 0;
   
   // Step 3: Try case-insensitive match
   for(int i = 0; i < totalSymbols; i++)
     {
      string symbolName = SymbolName(i, false);
      if(StringCompare(symbolName, masterSymbol, false) == 0)  // Case-insensitive
        {
         Print("Symbol matched (case-insensitive): '", masterSymbol, "' -> '", symbolName, "'");
         return symbolName;
        }
     }
   
   // Step 4: Find best similar symbol by comparing character similarity
   string masterUpper = masterSymbol;
   StringToUpper(masterUpper);
   
   for(int i = 0; i < totalSymbols; i++)
     {
      string symbolName = SymbolName(i, false);
      string symbolUpper = symbolName;
      StringToUpper(symbolUpper);
      
      // Calculate similarity score
      int score = 0;
      int minLen = MathMin(StringLen(masterUpper), StringLen(symbolUpper));
      
      // Check if master symbol is contained in slave symbol or vice versa
      if(StringFind(symbolUpper, masterUpper) >= 0 || StringFind(masterUpper, symbolUpper) >= 0)
        {
         score = minLen * 2;  // High score for substring match
        }
      else
        {
         // Count matching characters at same positions
         for(int j = 0; j < minLen; j++)
           {
            if(StringGetCharacter(masterUpper, j) == StringGetCharacter(symbolUpper, j))
               score++;
           }
        }
      
      // Prefer exact matches (after case-insensitive check)
      if(score > bestScore)
        {
         bestScore = score;
         bestMatch = symbolName;
        }
     }
   
   if(bestMatch != "" && bestScore > 0)
     {
      Print("Symbol matched (similarity): '", masterSymbol, "' -> '", bestMatch, "' (score: ", bestScore, ")");
      return bestMatch;
     }
   
   Print("ERROR: Could not find similar symbol for '", masterSymbol, "'");
   return "";
  }
//+------------------------------------------------------------------+
//| Get filling type for symbol                                       |
//+------------------------------------------------------------------+
ENUM_ORDER_TYPE_FILLING GetFillingType(string symbol)
  {
   int filling = (int)SymbolInfoInteger(symbol, SYMBOL_FILLING_MODE);
   
   if((filling & SYMBOL_FILLING_FOK) == SYMBOL_FILLING_FOK)
      return ORDER_FILLING_FOK;
   else if((filling & SYMBOL_FILLING_IOC) == SYMBOL_FILLING_IOC)
      return ORDER_FILLING_IOC;
   else
      return ORDER_FILLING_RETURN;
  }
//+------------------------------------------------------------------+
//| Execute open position trade                                       |
//+------------------------------------------------------------------+
ulong ExecuteOpenTrade(string symbol, string typeStr, double volume, double price, double sl, double tp, string comment)
  {
   Print("═══════════════════════════════════════════════════════════");
   Print(">>> EXECUTING OPEN TRADE <<<");
   Print("═══════════════════════════════════════════════════════════");
   
   // Validate symbol exists on slave broker
   if(!SymbolInfoInteger(symbol, SYMBOL_SELECT))
     {
      Print("ERROR: Symbol '", symbol, "' is not available on slave broker");
      return 0;
     }
   
   // Determine order type
   ENUM_ORDER_TYPE orderType;
   if(typeStr == "BUY" || typeStr == "0")
      orderType = ORDER_TYPE_BUY;
   else if(typeStr == "SELL" || typeStr == "1")
      orderType = ORDER_TYPE_SELL;
   else
     {
      Print("ERROR: Invalid type: ", typeStr);
      return 0;
     }
   
   // Get current market price
   double ask = SymbolInfoDouble(symbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(symbol, SYMBOL_BID);
   double currentPrice = (orderType == ORDER_TYPE_BUY) ? ask : bid;
   
   // Normalize volume
   double minLot = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
   double maxLot = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
   double lotStep = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);
   
   if(volume < minLot) volume = minLot;
   if(volume > maxLot) volume = maxLot;
   volume = MathFloor(volume / lotStep) * lotStep;
   
   // Calculate SL and TP
   double finalSL = 0;
   double finalTP = 0;
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   
   if(sl > 0)
     {
      finalSL = NormalizeDouble(sl, digits);
     }
   
   if(tp > 0)
     {
      finalTP = NormalizeDouble(tp, digits);
     }
   
   // Prepare trade request
   MqlTradeRequest request = {};
   MqlTradeResult result = {};
   
   request.action = TRADE_ACTION_DEAL;
   request.symbol = symbol;
   request.volume = volume;
   request.type = orderType;
   request.price = currentPrice;
   request.sl = finalSL;
   request.tp = finalTP;
   request.deviation = SlippagePoints;
   request.magic = SlaveMagicID;
   request.comment = comment;
   request.type_filling = GetFillingType(symbol);
   
   Print("TRADE REQUEST:");
   Print("  Symbol: ", request.symbol);
   Print("  Type: ", EnumToString(request.type));
   Print("  Volume: ", DoubleToString(request.volume, 2));
   Print("  Price: ", DoubleToString(request.price, digits));
   Print("  SL: ", (request.sl > 0 ? DoubleToString(request.sl, digits) : "Not set"));
   Print("  TP: ", (request.tp > 0 ? DoubleToString(request.tp, digits) : "Not set"));
   Print("  Magic: ", request.magic);
   
   // Execute trade
   ResetLastError();
   bool success = OrderSend(request, result);
   
   if(success && result.retcode == TRADE_RETCODE_DONE)
     {
      Print("═══════════════════════════════════════════════════════════");
      Print("✓✓✓ TRADE OPENED SUCCESSFULLY! ✓✓✓");
      Print("  Slave Ticket: ", result.order);
      Print("  Deal: ", result.deal);
      Print("  Volume: ", DoubleToString(result.volume, 2));
      Print("  Price: ", DoubleToString(result.price, digits));
      Print("═══════════════════════════════════════════════════════════");
      return result.order;  // Return slave ticket
     }
   else
     {
      Print("═══════════════════════════════════════════════════════════");
      Print("✗✗✗ TRADE OPEN FAILED! ✗✗✗");
      Print("  Error code: ", result.retcode);
      Print("  Error description: ", result.comment);
      Print("  Request ID: ", result.request_id);
      Print("═══════════════════════════════════════════════════════════");
      return 0;
     }
  }
//+------------------------------------------------------------------+
//| Close position by slave ticket                                    |
//+------------------------------------------------------------------+
bool ClosePositionByTicket(ulong slaveTicket)
  {
   Print("═══════════════════════════════════════════════════════════");
   Print(">>> CLOSING POSITION <<<");
   Print("  Slave Ticket: ", slaveTicket);
   Print("═══════════════════════════════════════════════════════════");
   
   // Check if position exists
   if(!PositionSelectByTicket(slaveTicket))
     {
      Print("WARNING: Position ", slaveTicket, " does not exist (may have been closed already)");
      return false;
     }
   
   // Get position details
   string posSymbol = PositionGetString(POSITION_SYMBOL);
   double posVolume = PositionGetDouble(POSITION_VOLUME);
   ENUM_POSITION_TYPE posType = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
   
   // Determine close order type (opposite of position type)
   ENUM_ORDER_TYPE closeType;
   if(posType == POSITION_TYPE_BUY)
      closeType = ORDER_TYPE_SELL;
   else
      closeType = ORDER_TYPE_BUY;
   
   // Get current market price
   double ask = SymbolInfoDouble(posSymbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(posSymbol, SYMBOL_BID);
   double closePrice = (closeType == ORDER_TYPE_SELL) ? bid : ask;
   
   // Prepare trade request
   MqlTradeRequest request = {};
   MqlTradeResult result = {};
   
   request.action = TRADE_ACTION_DEAL;
   request.position = slaveTicket;
   request.symbol = posSymbol;
   request.volume = posVolume;
   request.type = closeType;
   request.price = closePrice;
   request.deviation = SlippagePoints;
   request.magic = SlaveMagicID;
   request.comment = "Slave_Close";
   request.type_filling = GetFillingType(posSymbol);
   
   int digits = (int)SymbolInfoInteger(posSymbol, SYMBOL_DIGITS);
   Print("CLOSE REQUEST:");
   Print("  Position Ticket: ", slaveTicket);
   Print("  Symbol: ", request.symbol);
   Print("  Type: ", EnumToString(request.type));
   Print("  Volume: ", DoubleToString(request.volume, 2));
   Print("  Price: ", DoubleToString(request.price, digits));
   
   // Execute trade
   ResetLastError();
   bool success = OrderSend(request, result);
   
   if(success && result.retcode == TRADE_RETCODE_DONE)
     {
      Print("═══════════════════════════════════════════════════════════");
      Print("✓✓✓ POSITION CLOSED SUCCESSFULLY! ✓✓✓");
      Print("  Deal: ", result.deal);
      Print("  Volume: ", DoubleToString(result.volume, 2));
      Print("  Price: ", DoubleToString(result.price, digits));
      Print("═══════════════════════════════════════════════════════════");
      return true;
     }
   else
     {
      Print("═══════════════════════════════════════════════════════════");
      Print("✗✗✗ POSITION CLOSE FAILED! ✗✗✗");
      Print("  Error code: ", result.retcode);
      Print("  Error description: ", result.comment);
      Print("═══════════════════════════════════════════════════════════");
      return false;
     }
  }
//+------------------------------------------------------------------+
//| Store master ticket -> slave ticket mapping in cache              |
//+------------------------------------------------------------------+
void StoreTicketMapping(ulong masterTicket, ulong slaveTicket, string symbol)
  {
   // Check if mapping already exists
   int index = FindTicketMappingIndex(masterTicket);
   
   if(index >= 0)
     {
      // Update existing mapping
      ticketCache[index].slaveTicket = slaveTicket;
      ticketCache[index].symbol = symbol;
      ticketCache[index].openTime = TimeCurrent();
      Print("  Updated existing cache entry for master ticket: ", masterTicket);
     }
   else
     {
      // Add new mapping
      int size = ArraySize(ticketCache);
      ArrayResize(ticketCache, size + 1);
      ticketCache[size].masterTicket = masterTicket;
      ticketCache[size].slaveTicket = slaveTicket;
      ticketCache[size].symbol = symbol;
      ticketCache[size].openTime = TimeCurrent();
      Print("  Added new cache entry: Master ", masterTicket, " -> Slave ", slaveTicket);
     }
  }
//+------------------------------------------------------------------+
//| Get slave ticket from cache by master ticket                      |
//+------------------------------------------------------------------+
ulong GetSlaveTicketFromCache(ulong masterTicket)
  {
   int index = FindTicketMappingIndex(masterTicket);
   if(index >= 0)
     {
      return ticketCache[index].slaveTicket;
     }
   return 0;  // Not found
  }
//+------------------------------------------------------------------+
//| Find index of ticket mapping in cache                             |
//+------------------------------------------------------------------+
int FindTicketMappingIndex(ulong masterTicket)
  {
   for(int i = 0; i < ArraySize(ticketCache); i++)
     {
      if(ticketCache[i].masterTicket == masterTicket)
         return i;
     }
   return -1;  // Not found
  }
//+------------------------------------------------------------------+
//| Remove ticket mapping from cache                                  |
//+------------------------------------------------------------------+
void RemoveTicketMapping(ulong masterTicket)
  {
   int index = FindTicketMappingIndex(masterTicket);
   if(index >= 0)
     {
      int size = ArraySize(ticketCache);
      
      // Move last element to this position
      if(index < size - 1)
        {
         ticketCache[index] = ticketCache[size - 1];
        }
      
      // Resize array to remove last element
      ArrayResize(ticketCache, size - 1);
      Print("  Removed cache entry for master ticket: ", masterTicket);
     }
  }
//+------------------------------------------------------------------+

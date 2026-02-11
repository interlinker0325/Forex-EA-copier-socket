//+------------------------------------------------------------------+
//|                                                  mastersender.mq5 |
//|                                  Copyright 2025, MetaQuotes Ltd. |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+
#property copyright "Copyright 2025, MetaQuotes Ltd."
#property link      "https://www.mql5.com"
#property version   "1.00"

#include <Zmq/Zmq.mqh>

input int MagicID = 0;  // Set to 0 to track ALL positions, or set specific MagicID to filter
input string VPS_IP = "154.43.52.15";  // Slave IP address
input int VPS_Port = 5000;  // Slave port number

ulong trackedPositions[];  // Track position tickets to avoid duplicates

// ZMQ Context and Socket
Context zmqContext("PositionSender");
Socket zmqSocket(zmqContext, ZMQ_REQ);
string zmqConnectionString;  // ZMQ connection string (constructed from IP and Port)

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
  {
//---
   // Construct ZMQ connection string from IP and Port inputs
   zmqConnectionString = "tcp://" + VPS_IP + ":" + IntegerToString(VPS_Port);
   
   // Connect ZMQ socket
   Print("Connecting to ZMQ server...");
   if(!zmqSocket.connect(zmqConnectionString))
     {
      Print("ERROR: Failed to connect to ZMQ server at ", zmqConnectionString);
      return(INIT_FAILED);
     }
   Print("Successfully connected to ZMQ server");
   
   // Load current open positions to track them (so we don't process old positions)
   LoadCurrentPositions();
   
   Print("MasterSender initialized. MagicID: ", MagicID);
   Print("Currently tracking ", ArraySize(trackedPositions), " open positions");
//---
   return(INIT_SUCCEEDED);
  }
//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
  {
//---
   ArrayFree(trackedPositions);
   Print("MasterSender deinitialized.");
  }
//+------------------------------------------------------------------+
//| Expert tick function                                             |
//+------------------------------------------------------------------+
void OnTick()
  {
//---
   // OnTick() is not used - we only check positions when OnTrade() fires
   // This ensures we only detect positions when they are actually opened/closed
  }
//+------------------------------------------------------------------+
//| Trade event handler                                              |
//+------------------------------------------------------------------+
void OnTrade()
  {
   // OnTrade() fires when a trade event occurs (position opened/closed/modified)
   // This is the perfect time to check for new positions
   CheckNewPositions();
   CheckClosedPositions();
  }
//+------------------------------------------------------------------+
//| Check for newly opened positions                                 |
//+------------------------------------------------------------------+
void CheckNewPositions()
  {
   // Get all positions
   int total = PositionsTotal();
   
   if(total == 0)
      return;
   
   for(int i = total - 1; i >= 0; i--)
     {
      ulong posTicket = PositionGetTicket(i);
      if(posTicket == 0) continue;
      
      // Select the position to access its properties
      if(!PositionSelectByTicket(posTicket))
         continue;
      
      // Check MagicID only if MagicID is set (not 0)
      if(MagicID != 0)
        {
         long posMagic = PositionGetInteger(POSITION_MAGIC);
         if(posMagic != MagicID)
            continue;  // Skip if MagicID doesn't match
        }
      
      // Check if we're already tracking this position
      if(IsPositionTracked(posTicket))
         continue;  // Already tracked, skip
      
      // NEW POSITION FOUND!
      Print("========================================");
      Print(">>> NEW POSITION DETECTED! <<<");
      long posMagic = PositionGetInteger(POSITION_MAGIC);
      Print("    Ticket: ", posTicket);
      Print("    MagicID: ", posMagic);
      ProcessPositionOpen(posTicket);
      MarkPositionTracked(posTicket);
      Print("========================================");
     }
  }
//+------------------------------------------------------------------+
//| Check for closed positions                                      |
//+------------------------------------------------------------------+
void CheckClosedPositions()
  {
   // Check each tracked position to see if it still exists
   int trackedCount = ArraySize(trackedPositions);
   for(int i = trackedCount - 1; i >= 0; i--)
     {
      ulong posTicket = trackedPositions[i];
      
      // Check if position still exists
      if(!PositionSelectByTicket(posTicket))
        {
         // Position was closed!
         Print("========================================");
         Print(">>> POSITION CLOSED! <<<");
         ProcessPositionClose(posTicket);
         RemovePositionFromTracking(i);
         Print("========================================");
        }
     }
  }
//+------------------------------------------------------------------+
//| Process position open and send to VPS                            |
//+------------------------------------------------------------------+
void ProcessPositionOpen(ulong posTicket)
  {
   Print("--- ProcessPositionOpen() called for position ticket: ", posTicket, " ---");
   
   // Select the position
   if(!PositionSelectByTicket(posTicket))
     {
      Print("ERROR: Failed to select position ", posTicket);
      return;
     }
   
   // Get position data
   string symbol = PositionGetString(POSITION_SYMBOL);
   double volume = PositionGetDouble(POSITION_VOLUME);
   double price = PositionGetDouble(POSITION_PRICE_OPEN);
   double sl = PositionGetDouble(POSITION_SL);
   double tp = PositionGetDouble(POSITION_TP);
   datetime time = (datetime)PositionGetInteger(POSITION_TIME);
   string comment = PositionGetString(POSITION_COMMENT);
   
   // Get position type
   ENUM_POSITION_TYPE posType = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
   string typeStr = "";
   string actionStr = "OPEN";
   
   if(posType == POSITION_TYPE_BUY)
     {
      typeStr = "BUY";
     }
   else if(posType == POSITION_TYPE_SELL)
     {
      typeStr = "SELL";
     }
   else
     {
      Print("ERROR: Unknown position type: ", EnumToString(posType));
      return;
     }
   
   // Print detailed position specifications
   Print("═══════════════════════════════════════════════════════════");
   Print("POSITION SPECIFICATIONS:");
   Print("  ┌─ Ticket Number: ", posTicket);
   Print("  ├─ Symbol: ", symbol);
   Print("  ├─ Type: ", typeStr, " (", EnumToString(posType), ")");
   Print("  ├─ Action: ", actionStr);
   Print("  ├─ Volume: ", DoubleToString(volume, 2), " lots");
   Print("  ├─ Open Price: ", DoubleToString(price, DigitsFromString(symbol)));
   Print("  ├─ Stop Loss: ", (sl > 0 ? DoubleToString(sl, DigitsFromString(symbol)) : "Not set"));
   Print("  ├─ Take Profit: ", (tp > 0 ? DoubleToString(tp, DigitsFromString(symbol)) : "Not set"));
   Print("  ├─ Open Time: ", TimeToString(time, TIME_DATE|TIME_SECONDS));
   Print("  ├─ Comment: ", (comment != "" ? comment : "No comment"));
   Print("  └─ Magic ID: ", MagicID);
   Print("═══════════════════════════════════════════════════════════");
   
   // Send position data to VPS via ZMQ
   Print("Attempting to send position data to VPS via ZMQ...");
   SendPositionToVPS(time, posTicket, symbol, typeStr, actionStr, volume, price, sl, tp, comment);
  }
//+------------------------------------------------------------------+
//| Process position close and send to VPS                           |
//+------------------------------------------------------------------+
void ProcessPositionClose(ulong posTicket)
  {
   Print("--- ProcessPositionClose() called for position ticket: ", posTicket, " ---");
   
   // Get position data from history (position is already closed)
   if(!HistorySelect(0, TimeCurrent()))
     {
      Print("ERROR: Failed to select trade history");
      return;
     }
   
   // Find the position in history
   ulong positionID = 0;
   int total = HistoryDealsTotal();
   for(int i = total - 1; i >= 0; i--)
     {
      ulong dealTicket = HistoryDealGetTicket(i);
      if(dealTicket == 0) continue;
      
      // Check MagicID only if MagicID is set (not 0)
      if(MagicID != 0)
        {
         if(HistoryDealGetInteger(dealTicket, DEAL_MAGIC) != MagicID)
            continue;
        }
      
      ulong dealPosTicket = HistoryDealGetInteger(dealTicket, DEAL_POSITION_ID);
      if(dealPosTicket == posTicket)
        {
         positionID = HistoryDealGetInteger(dealTicket, DEAL_POSITION_ID);
         break;
        }
     }
   
   if(positionID == 0)
     {
      Print("WARNING: Could not find position ", posTicket, " in history");
      // Send to VPS with minimal data
      SendPositionToVPS(TimeCurrent(), posTicket, "UNKNOWN", "CLOSE", "CLOSE", 0, 0, 0, 0, "Position closed");
      return;
     }
   
   // Get position data from history
   if(HistorySelectByPosition(positionID))
     {
      int posDeals = HistoryDealsTotal();
      string symbol = "";
      string typeStr = "";
      double volume = 0;
      double price = 0;
      datetime time = TimeCurrent();
      ENUM_DEAL_TYPE openDealType = WRONG_VALUE;
      
      // First pass: get opening deal info
      for(int i = 0; i < posDeals; i++)
        {
         ulong dealTicket = HistoryDealGetTicket(i);
         if(dealTicket == 0) continue;
         
         ENUM_DEAL_TYPE dealType = (ENUM_DEAL_TYPE)HistoryDealGetInteger(dealTicket, DEAL_TYPE);
         
         // Get symbol and type from entry deal (first BUY or SELL)
         if((dealType == DEAL_TYPE_BUY || dealType == DEAL_TYPE_SELL) && symbol == "")
           {
            symbol = HistoryDealGetString(dealTicket, DEAL_SYMBOL);
            volume = HistoryDealGetDouble(dealTicket, DEAL_VOLUME);
            time = (datetime)HistoryDealGetInteger(dealTicket, DEAL_TIME);
            openDealType = dealType;
            
            if(dealType == DEAL_TYPE_BUY)
               typeStr = "BUY";
            else
               typeStr = "SELL";
           }
        }
      
      // Second pass: find closing deal (opposite type from opening)
      ENUM_DEAL_TYPE closeDealType = (openDealType == DEAL_TYPE_BUY) ? DEAL_TYPE_SELL : DEAL_TYPE_BUY;
      
      for(int i = posDeals - 1; i >= 0; i--)
        {
         ulong dealTicket = HistoryDealGetTicket(i);
         if(dealTicket == 0) continue;
         
         ENUM_DEAL_TYPE dealType = (ENUM_DEAL_TYPE)HistoryDealGetInteger(dealTicket, DEAL_TYPE);
         
         // Get close price from closing deal (opposite type from opening)
         if(dealType == closeDealType)
           {
            price = HistoryDealGetDouble(dealTicket, DEAL_PRICE);
            // Use close time if available
            datetime closeTime = (datetime)HistoryDealGetInteger(dealTicket, DEAL_TIME);
            if(closeTime > time)
               time = closeTime;
            break;  // Found closing deal, exit loop
           }
        }
      
      // If close price still 0, try to get from last deal
      if(price == 0 && posDeals > 0)
        {
         ulong lastDealTicket = HistoryDealGetTicket(posDeals - 1);
         if(lastDealTicket != 0)
           {
            price = HistoryDealGetDouble(lastDealTicket, DEAL_PRICE);
           }
        }
      
      Print("Position Close Specifications:");
      Print("  Ticket: ", posTicket);
      Print("  Symbol: ", symbol);
      Print("  Type: ", typeStr);
      Print("  Volume: ", volume);
      Print("  Close Price: ", price);
      Print("  Time: ", TimeToString(time, TIME_DATE|TIME_SECONDS));
      
      // Send position close data to VPS via ZMQ
      Print("Attempting to send position close data to VPS via ZMQ...");
      SendPositionToVPS(time, posTicket, symbol, typeStr, "CLOSE", volume, price, 0, 0, "Position closed");
     }
   
   Print("--- ProcessPositionClose() finished for ticket: ", posTicket, " ---");
  }
//+------------------------------------------------------------------+
//| Check if position is already tracked                             |
//+------------------------------------------------------------------+
bool IsPositionTracked(ulong posTicket)
  {
   int size = ArraySize(trackedPositions);
   for(int i = 0; i < size; i++)
     {
      if(trackedPositions[i] == posTicket)
         return true;
     }
   return false;
  }
//+------------------------------------------------------------------+
//| Mark position as tracked                                          |
//+------------------------------------------------------------------+
void MarkPositionTracked(ulong posTicket)
  {
   int size = ArraySize(trackedPositions);
   ArrayResize(trackedPositions, size + 1);
   trackedPositions[size] = posTicket;
   Print("Position ", posTicket, " added to tracking list. Total tracked: ", ArraySize(trackedPositions));
  }
//+------------------------------------------------------------------+
//| Remove position from tracking                                    |
//+------------------------------------------------------------------+
void RemovePositionFromTracking(int index)
  {
   int size = ArraySize(trackedPositions);
   if(index < 0 || index >= size)
      return;
   
   // Move all elements after index one position forward
   for(int i = index; i < size - 1; i++)
     {
      trackedPositions[i] = trackedPositions[i + 1];
     }
   
   ArrayResize(trackedPositions, size - 1);
   Print("Position removed from tracking. Total tracked: ", ArraySize(trackedPositions));
  }
//+------------------------------------------------------------------+
//| Load current open positions on startup                           |
//+------------------------------------------------------------------+
void LoadCurrentPositions()
  {
   ArrayResize(trackedPositions, 0);
   
   int total = PositionsTotal();
   Print("Loading current open positions. Total positions: ", total, " | MagicID filter: ", (MagicID == 0 ? "ALL" : IntegerToString(MagicID)));
   
   for(int i = 0; i < total; i++)
     {
      ulong posTicket = PositionGetTicket(i);
      if(posTicket == 0) continue;
      
      // Select the position to access its properties
      if(!PositionSelectByTicket(posTicket))
         continue;
      
      // Check MagicID only if MagicID is set (not 0)
      if(MagicID != 0)
        {
         if(PositionGetInteger(POSITION_MAGIC) != MagicID)
            continue;  // Skip if MagicID doesn't match
        }
      
      // Track this position
      MarkPositionTracked(posTicket);
      string symbol = PositionGetString(POSITION_SYMBOL);
      Print("Loaded existing position: ", posTicket, " | Symbol: ", symbol);
     }
   
   Print("Loaded ", ArraySize(trackedPositions), " existing positions to track");
  }
//+------------------------------------------------------------------+
//| Get digits from symbol string                                    |
//+------------------------------------------------------------------+
int DigitsFromString(string symbol)
  {
   double point = SymbolInfoDouble(symbol, SYMBOL_POINT);
   if(point == 0) return 5;
   
   int digits = 0;
   while(point < 1)
     {
      point *= 10;
      digits++;
     }
   return digits;
  }
//+------------------------------------------------------------------+
//| Convert position data to JSON format                             |
//+------------------------------------------------------------------+
string PositionToJson(datetime time, ulong ticket, string symbol, string typeStr, string actionStr, 
                      double volume, double price, double sl, double tp, string comment)
  {
   string json = "{";
   json += "\"Time\":\"" + TimeToString(time, TIME_DATE|TIME_SECONDS) + "\",";
   json += "\"Ticket\":" + IntegerToString(ticket) + ",";
   json += "\"Symbol\":\"" + symbol + "\",";
   json += "\"Type\":\"" + typeStr + "\",";
   json += "\"Action\":\"" + actionStr + "\",";
   json += "\"Volume\":" + DoubleToString(volume, 2) + ",";
   json += "\"Price\":" + DoubleToString(price, DigitsFromString(symbol)) + ",";
   json += "\"SL\":" + DoubleToString(sl, DigitsFromString(symbol)) + ",";
   json += "\"TP\":" + DoubleToString(tp, DigitsFromString(symbol)) + ",";
   json += "\"Comment\":\"" + comment + "\",";
   json += "\"MagicID\":" + IntegerToString(MagicID);
   json += "}";
   return json;
  }
//+------------------------------------------------------------------+
//| Send position data to VPS via ZMQ Socket                         |
//+------------------------------------------------------------------+
void SendPositionToVPS(datetime time, ulong ticket, string symbol, string typeStr, string actionStr, 
                       double volume, double price, double sl, double tp, string comment)
  {
   Print("───────────────────────────────────────────────────────────");
   Print("ZMQ SOCKET OPERATION:");
   Print("  Connection: ", zmqConnectionString);
   
   // Convert position data to JSON
   string jsonData = PositionToJson(time, ticket, symbol, typeStr, actionStr, volume, price, sl, tp, comment);
   Print("  JSON Data: ", jsonData);
   
   // Create ZMQ message with JSON data
   ZmqMsg request(jsonData);
   
   // Send message via ZMQ socket
   Print("  Sending message to: ", zmqConnectionString);
   if(!zmqSocket.send(request))
     {
      Print("  ✗✗✗ ERROR: ZMQ send failed! ✗✗✗");
      Print("  Failed to send position data");
      ResetLastError();
     }
   else
     {
      Print("  ✓✓✓ SUCCESS: ZMQ message sent! ✓✓✓");
      Print("  Sent: ", symbol, " ", typeStr, " ", actionStr, " | Ticket:", ticket);
      
      // Wait for reply with timeout (polling approach)
      // Note: ZMQ REQ sockets must receive after sending, but we use polling
      // to avoid blocking indefinitely if the server doesn't respond
      ZmqMsg reply;
      bool replyReceived = false;
      int timeoutMs = 10000;  // 10 second timeout (increased for slower processing)
      int elapsedMs = 0;
      int pollInterval = 50;  // Check every 50ms (balance between responsiveness and CPU usage)
      
      Print("  Waiting for server reply (timeout: ", timeoutMs, "ms)...");
      
      while(elapsedMs < timeoutMs && !replyReceived)
        {
         // Try non-blocking receive
         if(zmqSocket.recv(reply, true))  // true = non-blocking (DONTWAIT)
           {
            replyReceived = true;
            string response = reply.getData();
            Print("  ✓✓✓ Server Response Received: ", response);
            break;
           }
         Sleep(pollInterval);
         elapsedMs += pollInterval;
        }
      
      if(!replyReceived)
        {
         Print("  ⚠⚠⚠ WARNING: No reply received within ", timeoutMs, "ms timeout");
         Print("  Server may still be processing the request, or connection issue");
         Print("  Check slave EA logs to verify if message was received");
        }
     }
   
   Print("───────────────────────────────────────────────────────────");
  }
//+------------------------------------------------------------------+


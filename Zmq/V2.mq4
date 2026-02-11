//+------------------------------------------------------------------+
//|                       Cyborg_Bridge_V4_FIXED.mq4                 |
//|               (Pairs with Python V4 Universal GUI)               |
//+------------------------------------------------------------------+
#property copyright "Cyborg Team"
#property strict

// YOU MUST HAVE THE ZMQ LIBRARY INSTALLED
#include <Zmq/Zmq.mqh> 

// --- SETTINGS ---
input int    Server_Port     = 5555;      // Must match Python Port
input int    Slippage_Pips   = 3;
input int    MagicNumber     = 888;
input bool   Show_Debug      = true;
ZmqMsg msg;

// --- ZMQ VARIABLES ---
Context context("CyborgContext");
Socket  socket(context, ZMQ_REP); // REP = Server (Replies to Python)

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
   Print(">>> CYBORG BRIDGE V4 (FIXED): STARTING SERVER ON PORT ", Server_Port);
   
   // 1. Bind the Socket (MT4 is the Server)
   if(!socket.bind("tcp://*:" + IntegerToString(Server_Port))) {
      Print("!!! ERROR: Could not bind to port. Is another EA running?");
      return(INIT_FAILED);
   }
   
   // 2. Set specific ZMQ options for speed
   socket.setLinger(0);
   
   // 3. Start High-Speed Timer (Check for Python commands every 1ms)
   EventSetMillisecondTimer(1); 
   
   return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   EventKillTimer();
   Print(">>> CYBORG BRIDGE OFFLINE.");
}

//+------------------------------------------------------------------+
//| TIMER LOOP (Replaces OnTick for smoother communication)          |
//+------------------------------------------------------------------+
void OnTimer()
{
   // 1. Check if Python sent a message (Non-blocking)
   if(!socket.recv(msg, 1)) return;
   string request = msg.getData();
   
   // 2. HANDLE "GET_TICK" COMMAND
   if(request == "GET_TICK")
   {
      // Python wants price. Send it immediately.
      string reply = "TICK|" + DoubleToString(Bid, 5);
      socket.send(reply);
      return;
   }
   
   // 3. HANDLE "TRADE" COMMANDS
   // Python Format: "TRADE|OPEN|TYPE|LOTS|PRICE|SL|TP|TAG"
   // Index Map:       0  |  1 |  2 |  3 |  4  | 5| 6| 7
   if(StringFind(request, "TRADE") >= 0)
   {
      if(Show_Debug) Print(">>> RECEIVED COMMAND: ", request);
      
      // Parse the command
      string parts[];
      StringSplit(request, '|', parts);
      
      // Check structure (Needs at least 8 parts for full safety)
      if(ArraySize(parts) >= 8) 
      {
         int cmd_type    = (int)StringToInteger(parts[2]); // 0=OP_BUY, 1=OP_SELL
         double lots     = StringToDouble(parts[3]);
         // parts[4] is Price, but we use Ask/Bid for instant execution
         double sl_price = StringToDouble(parts[5]);       // FIX: Get SL from Python
         double tp_price = StringToDouble(parts[6]);       // FIX: Get TP from Python
         string comment  = parts[7];                       // FIX: Get Tag from Python
         
         // EXECUTE WITH SAFETY
         int ticket = ExecuteTrade(cmd_type, lots, sl_price, tp_price, comment);
         
         // SEND REPLY BACK TO PYTHON
         if(ticket > 0) {
            socket.send("SUCCESS|" + IntegerToString(ticket));
         } else {
            socket.send("ERROR|" + IntegerToString(GetLastError()));
         }
      }
      else
      {
         if(Show_Debug) Print("!!! ERROR: Command too short. Got parts: ", ArraySize(parts));
         socket.send("ERROR|BAD_FORMAT");
      }
   }
}

//+------------------------------------------------------------------+
//| Helper: Execute Trade (NOW ACCEPTS SL, TP, COMMENT)              |
//+------------------------------------------------------------------+
int ExecuteTrade(int type, double volume, double sl, double tp, string comment)
{
   double price;
   color arrow_color;
   int cmd;
   
   if(type == 0) {
      cmd = OP_BUY;
      price = Ask;
      arrow_color = clrGreen;
   } else {
      cmd = OP_SELL;
      price = Bid;
      arrow_color = clrRed;
   }
   
   // Send Order with ACTUAL SL/TP and COMMENT
   int ticket = OrderSend(Symbol(), cmd, volume, price, Slippage_Pips*10, sl, tp, comment, MagicNumber, 0, arrow_color);
   
   if(ticket < 0) Print("!!! ORDER ERROR: ", GetLastError());
   else           Print(">>> ORDER OPENED: #", ticket, " SL:", sl, " TP:", tp);
   
   return ticket;
}

//+------------------------------------------------------------------+
//| Helper: String Split Function                                    |
//+------------------------------------------------------------------+
void StringSplit(string s, int separator, string &result[])
{
   ushort u_sep = (ushort)separator;
   StringSplit(s, u_sep, result);
}

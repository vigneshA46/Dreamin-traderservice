from datetime import time

DELTA_CE = 0.65
DELTA_PE = -0.65
MTM_TARGET = 3000
MTM_STOP = -3000
EXIT_TIME = time(15, 15)

class VWAPStrategy:
    def __init__(self):
        self.position = None   # 'CE' or 'PE'
        self.entry_price = None
        self.entry_time = None
        self.mtm_locked = False
        self.pnl = 0

    def on_candle(self, row):
        """
        row: pandas Series with
        close, vwap, datetime
        """

        time_now = row.name.time()
        price = row['close']
        vwap = row['vwap']

        # ===== NO POSITION =====
        if self.position is None and not self.mtm_locked:
            if price > vwap:
                return self._enter('CE', price, row.name)
            elif price < vwap:
                return self._enter('PE', price, row.name)

        # ===== POSITION OPEN =====
        if self.position:
            delta = DELTA_CE if self.position == 'CE' else DELTA_PE
            self.pnl = (price - self.entry_price) * delta * 50  # NIFTY lot

            # MTM exit
            if self.pnl >= MTM_TARGET or self.pnl <= MTM_STOP:
                self.mtm_locked = True
                return self._exit(price, row.name, 'MTM')

            # Time exit
            if time_now >= EXIT_TIME:
                self.mtm_locked = True
                return self._exit(price, row.name, 'TIME')

            # VWAP exit
            if self.position == 'CE' and price < vwap:
                return self._exit(price, row.name, 'VWAP')
            if self.position == 'PE' and price > vwap:
                return self._exit(price, row.name, 'VWAP')

        return None

    def _enter(self, side, price, dt):
        self.position = side
        self.entry_price = price
        self.entry_time = dt
        self.pnl = 0
        return f"{dt} ENTRY {side} @ {price}"

    def _exit(self, price, dt, reason):
        msg = (
            f"{dt} EXIT {self.position} @ {price} | "
            f"PNL: {round(self.pnl,2)} | {reason}"
        )
        self.position = None
        self.entry_price = None
        self.entry_time = None
        self.pnl = 0
        return msg
     
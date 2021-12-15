package com.trading.bot.enums;

import com.trading.bot.util.DFUtil;

public enum ColumnsNamesEnum {
    CANDLE_INDEX {
        @Override
        public String toString() {
            return DFUtil.toColumnString(super.toString());
        }
    },
    DATE_AND_TIME {
        @Override
        public String toString() {
            return DFUtil.toColumnString(super.toString());
        }
    },
    OPEN {
        @Override
        public String toString() {
            return DFUtil.toColumnString(super.toString());
        }
    },
    CLOSE {
        @Override
        public String toString() {
            return DFUtil.toColumnString(super.toString());
        }
    },
    HIGH {
        @Override
        public String toString() {
            return DFUtil.toColumnString(super.toString());
        }
    },
    LOW {
        @Override
        public String toString() {
            return DFUtil.toColumnString(super.toString());
        }
    },
    VOLUME {
        @Override
        public String toString() {
            return DFUtil.toColumnString(super.toString());
        }
    },
    STOP_LOSS {
        @Override
        public String toString() {
            return DFUtil.toColumnString(super.toString());
        }
    },
    ENTRY {
        @Override
        public String toString() {
            return DFUtil.toColumnString(super.toString());
        }
    },
    TAKE_PROFIT {
        @Override
        public String toString() {
            return DFUtil.toColumnString(super.toString());
        }
    },
    TREAD_SIGNAL {
        @Override
        public String toString() {
            return DFUtil.toColumnString(super.toString());
        }
    },
    DEFAULT_HIDDEN_COLUMN_PREFIX {
        @Override
        public String toString() {
            return DFUtil.toColumnString(super.toString());
        }
    }
}

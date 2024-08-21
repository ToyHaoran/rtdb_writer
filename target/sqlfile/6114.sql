select P_NUM, AV, AVR, Q, BF, FQ, FAI, MS, TEW, CST from root.sg.unit0.historyA.d* where time < 1970-01-02T08:00:00 order by time asc align by device
select P_NUM, AV, AVR, Q, BF, FQ, FAI, MS, TEW, CST from root.sg.unit0.historyA.d* where time < 1970-01-03T08:00:00 and time >= 1970-01-02T08:00:00 order by time asc align by device
select P_NUM, AV, AVR, Q, BF, FQ, FAI, MS, TEW, CST from root.sg.unit0.historyA.d* where time < 1970-01-04T08:00:00 and time >= 1970-01-03T08:00:00 order by time asc align by device
select P_NUM, AV, AVR, Q, BF, FQ, FAI, MS, TEW, CST from root.sg.unit0.historyA.d* where time < 1970-01-05T08:00:00 and time >= 1970-01-04T08:00:00 order by time asc align by device
select P_NUM, DV, DVR, Q, BF, FQ, FAI, MS, TEW, CST from root.sg.unit0.historyD.d* where time < 1970-01-01T20:00:00 order by time asc align by device
select P_NUM, DV, DVR, Q, BF, FQ, FAI, MS, TEW, CST from root.sg.unit0.historyD.d* where time < 1970-01-02T08:00:00 and time >= 1970-01-01T20:00:00 order by time asc align by device
select P_NUM, DV, DVR, Q, BF, FQ, FAI, MS, TEW, CST from root.sg.unit0.historyD.d* where time < 1970-01-02T20:00:00 and time >= 1970-01-02T08:00:00 order by time asc align by device
select P_NUM, DV, DVR, Q, BF, FQ, FAI, MS, TEW, CST from root.sg.unit0.historyD.d* where time < 1970-01-03T08:00:00 and time >= 1970-01-02T20:00:00 order by time asc align by device
select P_NUM, DV, DVR, Q, BF, FQ, FAI, MS, TEW, CST from root.sg.unit0.historyD.d* where time < 1970-01-03T20:00:00 and time >= 1970-01-03T08:00:00 order by time asc align by device
select P_NUM, DV, DVR, Q, BF, FQ, FAI, MS, TEW, CST from root.sg.unit0.historyD.d* where time < 1970-01-04T08:00:00 and time >= 1970-01-03T20:00:00 order by time asc align by device
select P_NUM, DV, DVR, Q, BF, FQ, FAI, MS, TEW, CST from root.sg.unit0.historyD.d* where time < 1970-01-04T20:00:00 and time >= 1970-01-04T08:00:00 order by time asc align by device
select P_NUM, DV, DVR, Q, BF, FQ, FAI, MS, TEW, CST from root.sg.unit0.historyD.d* where time < 1970-01-05T08:00:00 and time >= 1970-01-04T20:00:00 order by time asc align by device
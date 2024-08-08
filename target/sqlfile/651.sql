select P_NUM, AV, AVR, Q, BF, FQ, FAI, MS, TEW, CST from root.sg.unit0.fastA order by time desc limit 100
select P_NUM, DV, DVR, Q, BF, FQ, FAI, MS, TEW, CST from root.sg.unit0.fastD order by time desc limit 100
select P_NUM, AV, AVR, Q, BF, FQ, FAI, MS, TEW, CST from root.sg.unit0.normalA* order by time desc limit 100 align by device
select P_NUM, DV, DVR, Q, BF, FQ, FAI, MS, TEW, CST from root.sg.unit0.normalD* order by time desc limit 100 align by device

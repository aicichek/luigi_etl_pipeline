SELECT T1.[_Period],T1.[_LineNo] as Pline,
--d.[_Number], 
subquery.pNumber,
T2.[_Code] as ct,
T1.[_Fld1051] as sumrub, T1.[_Fld1048] as sumdol,
--subquery.ssrub, 
subquery.ssdol, subquery.qty,
 r2.[_Description] as group_customer, r3.[_Description] as brand, r4.[_Description] as tovtype, r5.[_Description] as tovsubtype, r6.[_Description] as salesdep,
r7.[_Description] as Sales_manager, r8.[_Description] as client, r9.[_Description] as city, r11.[_Description] as clienttype, subquery.Sklad as sklad, subquery.Nomenklatura as nomenklatura,
subquery.Product_manager as Product_manager, subquery.Product_type as product_type,
CASE
	WHEN c.ParentCustomer_ID = 0 THEN c.FullName
	ELSE (
	    SELECT TOP 1 c_parent.FullName
	    FROM shops.dbo.Customer c_parent
	    WHERE c_parent.Customer_ID = c.ParentCustomer_ID
	    ORDER BY c_parent.Customer_ID
	    )
END AS GroupName,
CASE
    WHEN c.ParentCustomer_ID IN (111946, 111936) THEN 'Фед.дистр'
    WHEN c.Customer_Type_ID = 22 THEN 'РАЭК'
    WHEN (c.division_id = 6 OR c.division_id = 4) AND c.ParentCustomer_ID NOT IN (111946, 111936) AND c.Customer_Type_ID != 22 THEN 'Рег.дистр.'
    WHEN c.division_id = 19 AND c.Region_ID != 10013012 THEN 'Дистр.СНГ'
    WHEN c.Customer_Type_ID = 7 AND c.Customer_Group_ID != 6 THEN 'DIY сети'
    WHEN c.Customer_Type_ID IN (8, 10) AND c.Customer_Group_ID != 6 THEN 'Food сети'
    WHEN c.Customer_Group_ID = 6 THEN 'МП'
    WHEN c.Customer_Type_ID = 15 THEN 'OEM'
    ELSE 'прочие'
END AS ClientTypeName
FROM IV2012_BT.dbo.[_AccRg1029] T1
INNER JOIN IV2012_BT.dbo.[_Acc18] T2 ON T2._IDRRef = T1.[_AccountCtRRef]
INNER JOIN IV2012_BT.dbo.[_Acc18] T3 ON T3._IDRRef = T1.[_AccountDtRRef]
INNER JOIN IV2012_BT.dbo.[_Document227] d ON d.[_IDRRef] = t1.[_RecorderRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r ON T1.[_Fld1033Ct_RRRef] = r.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r2 ON T1.[_Fld1034Ct_RRRef] = r2.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r3 ON T1.[_Fld1035Ct_RRRef] = r3.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r4 ON T1.[_Fld1036Ct_RRRef] = r4.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r5 ON T1.[_Fld1037Ct_RRRef] = r5.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r6 ON T1.[_Fld1038Ct_RRRef] = r6.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r7 ON T1.[_Fld1039Ct_RRRef] = r7.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r8 ON T1.[_Fld1040Ct_RRRef] = r8.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r9 ON T1.[_Fld1041Ct_RRRef] = r9.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r11 ON T1.[_Fld1043Ct_RRRef] = r11.[_IDRRef]
LEFT JOIN SHOPS.dbo.CUSTOMER c 
    ON r.[_Code] = c.Customer_ID 
    OR r2.[_Code] = c.Customer_ID 
    OR r3.[_Code] = c.Customer_ID 
    OR r4.[_Code] = c.Customer_ID
    OR r5.[_Code] = c.Customer_ID 
    OR r6.[_Code] = c.Customer_ID 
    OR r7.[_Code] = c.Customer_ID 
    OR r8.[_Code] = c.Customer_ID
    OR r9.[_Code] = c.Customer_ID 
    OR r11.[_Code] = c.Customer_ID
INNER JOIN (
SELECT F1.[_Period] , F1.[_LineNo] as Pline, d2.[_Number] as Pnumber, F3.[_Code] as Дт, F2.[_Code] as Кт,-- F1.[_Fld1051] as ssrub,
F1.[_Fld1048] as ssdol, F1.[_Fld1050] as qty, rf.[_Code] as ID_Sklad, rf.[_Description] as Sklad, rf2.[_Code] as ID_Nomenklatura,
rf2.[_Description] as Nomenklatura, fw.Worker_ID as Product_Manager_ID, fw.Worker_Name as Product_Manager, at2.Type_Name as Product_type
FROM IV2012_BT.dbo.[_AccRg1029] F1
INNER JOIN IV2012_BT.dbo.[_Acc18] F2 ON F2._IDRRef = F1.[_AccountCtRRef]
INNER JOIN IV2012_BT.dbo.[_Acc18] F3 ON F3._IDRRef = F1.[_AccountDtRRef]
INNER JOIN IV2012_BT.dbo.[_Document227] d2 ON d2.[_IDRRef] = F1.[_RecorderRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] rf ON F1.[_Fld1033Ct_RRRef] = rf.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] rf2 ON F1.[_Fld1035Ct_RRRef] = rf2.[_IDRRef]
LEFT JOIN DMR_Sklad.dbo.assortim a ON rf2.[_Code] = a.TovCode
LEFT JOIN DMR_Sklad.dbo.cer_Data_Worker cdw ON a.TovCode = cdw.TovCode
LEFT JOIN DMR_Sklad.dbo.f_workers fw ON cdw.Worker_ID = fw.Worker_ID
LEFT JOIN DMR_Sklad.dbo.assortim_type at2 ON a.Type_ID = at2.Type_ID
WHERE (F2._Code = '45')
AND F1.[_Period] BETWEEN '4021.01.01' and '4024.01.01' AND F3.[_Code] = '90.2'
)as subquery
ON d.[_Number] = subquery.Pnumber AND t1.[_LineNo] + 1 = subquery.Pline
WHERE T2._Code = '90.1' AND T1.[_Period] BETWEEN '4021.01.01' and '4024.01.01'
AND r.[_Description] = 'Доходы от реализации'
AND T3.[_Code] != '70.2'
UNION ALL
SELECT T1.[_Period],T1.[_LineNo], 
--d.[_Number] as Pnumber, 
subquery.pNumber,
T2.[_Code] as ct,
T1.[_Fld1051] as sumrub, T1.[_Fld1048] as sumdol,
--subquery.ssrub, 
subquery.ssdol, subquery.qty,
--r.[_Code] , r.[_Description] , 
r2.[_Description], r3.[_Description], r4.[_Description],r5.[_Description], r6.[_Description],
r7.[_Description],r8.[_Description], r9.[_Description],
r11.[_Description], subquery.Sklad, subquery.Nomenklatura,
subquery.Product_manager, subquery.Product_type, 
CASE
	WHEN c.ParentCustomer_ID = 0 THEN c.FullName
	ELSE (
	    SELECT TOP 1 c_parent.FullName
	    FROM shops.dbo.Customer c_parent
        WHERE c_parent.Customer_ID = c.ParentCustomer_ID
        ORDER BY c_parent.Customer_ID
	    )
END AS GroupName,
CASE
    WHEN c.ParentCustomer_ID IN (111946, 111936) THEN 'Фед.дистр'
    WHEN c.Customer_Type_ID = 22 THEN 'РАЭК'
    WHEN (c.division_id = 6 OR c.division_id = 4) AND c.ParentCustomer_ID NOT IN (111946, 111936) AND c.Customer_Type_ID != 22 THEN 'Рег.дистр.'
    WHEN c.division_id = 19 AND c.Region_ID != 10013012 THEN 'Дистр.СНГ'
    WHEN c.Customer_Type_ID = 7 AND c.Customer_Group_ID != 6 THEN 'DIY сети'
    WHEN c.Customer_Type_ID IN (8, 10) AND c.Customer_Group_ID != 6 THEN 'Food сети'
    WHEN c.Customer_Group_ID = 6 THEN 'МП'
    WHEN c.Customer_Type_ID = 15 THEN 'OEM'
    ELSE 'прочие'
END AS ClientType
FROM IV2012_BT.dbo.[_AccRg1029] T1
INNER JOIN IV2012_BT.dbo.[_Acc18] T2 ON T2._IDRRef = T1.[_AccountCtRRef]
INNER JOIN IV2012_BT.dbo.[_Acc18] T3 ON T3._IDRRef = T1.[_AccountDtRRef]
INNER JOIN IV2012_BT.dbo.[_Document227] d ON d.[_IDRRef] = t1.[_RecorderRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r ON T1.[_Fld1033Ct_RRRef] = r.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r2 ON T1.[_Fld1034Ct_RRRef] = r2.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r3 ON T1.[_Fld1035Ct_RRRef] = r3.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r4 ON T1.[_Fld1036Ct_RRRef] = r4.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r5 ON T1.[_Fld1037Ct_RRRef] = r5.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r6 ON T1.[_Fld1038Ct_RRRef] = r6.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r7 ON T1.[_Fld1039Ct_RRRef] = r7.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r8 ON T1.[_Fld1040Ct_RRRef] = r8.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r9 ON T1.[_Fld1041Ct_RRRef] = r9.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] r11 ON T1.[_Fld1043Ct_RRRef] = r11.[_IDRRef]
LEFT JOIN SHOPS.dbo.CUSTOMER c 
    ON r.[_Code] = c.Customer_ID 
    OR r2.[_Code] = c.Customer_ID 
    OR r3.[_Code] = c.Customer_ID 
    OR r4.[_Code] = c.Customer_ID
    OR r5.[_Code] = c.Customer_ID 
    OR r6.[_Code] = c.Customer_ID 
    OR r7.[_Code] = c.Customer_ID 
    OR r8.[_Code] = c.Customer_ID
    OR r9.[_Code] = c.Customer_ID 
    OR r11.[_Code] = c.Customer_ID
INNER JOIN (
SELECT F1.[_Period],F1.[_LineNo] as Pline, d2.[_Number] as Pnumber, F3.[_Code] as Дт, F2.[_Code] as Кт, --F1.[_Fld1051] as ssrub,
F1.[_Fld1048] as ssdol, F1.[_Fld1050] as qty, rf.[_Code] as ID_Sklad, rf.[_Description] as Sklad, rf2.[_Code] as ID_Nomenklatura,
rf2.[_Description] as Nomenklatura, fw.Worker_ID as Product_Manager_ID, fw.Worker_Name as Product_Manager, at2.Type_Name as Product_type
FROM IV2012_BT.dbo.[_AccRg1029] F1
INNER JOIN IV2012_BT.dbo.[_Acc18] F2 ON F2._IDRRef = F1.[_AccountCtRRef]
INNER JOIN IV2012_BT.dbo.[_Acc18] F3 ON F3._IDRRef = F1.[_AccountDtRRef]
INNER JOIN IV2012_BT.dbo.[_Document227] d2 ON d2.[_IDRRef] = F1.[_RecorderRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] rf ON F1.[_Fld1033Ct_RRRef] = rf.[_IDRRef]
LEFT JOIN IV2012_BT.dbo.[_Reference64] rf2 ON F1.[_Fld1034Ct_RRRef] = rf2.[_IDRRef]
LEFT JOIN DMR_Sklad.dbo.assortim a ON rf2.[_Code] = a.TovCode
LEFT JOIN DMR_Sklad.dbo.cer_Data_Worker cdw ON a.TovCode = cdw.TovCode
LEFT JOIN DMR_Sklad.dbo.f_workers fw ON cdw.Worker_ID = fw.Worker_ID
LEFT JOIN DMR_Sklad.dbo.assortim_type at2 ON a.Type_ID = at2.Type_ID
WHERE (F2._Code = '41.1' OR F2._Code = '41.3')
AND F1.[_Period] BETWEEN '4021.01.01' and '4024.01.01' AND F3.[_Code] = '90.2'
)as subquery
ON d.[_Number] = subquery.Pnumber AND t1.[_LineNo] + 1 = subquery.Pline
WHERE T2._Code = '90.1' AND T1.[_Period] BETWEEN '4021.01.01' and '4024.01.01'
AND r.[_Description] = 'Доходы от реализации'
AND T3.[_Code] != '70.2'
UNION ALL
SELECT
    f1.[_Period],
    f1.[_LineNo],
    d2.[_Number] AS pNumber,
    f2.[_Code] AS ct,
    rs.Real_String_Amount * rs.Real_String_PriceR AS sumrub,
    rs.Real_String_Amount * rs.Real_String_Price AS sumdol,
    rs.Real_String_Amount * rs.Real_String_PriceSS AS ssdol,
    rs.Real_String_Amount AS qty,
    tovgr.TovGrName,
    tovpgr.TovPGrName,
    spgr.TovSPGrName,
    upgr.TovUPGrName,
    'Отдел по работе с сетевыми и розничными клиентами',
    fw.Worker_Name,
    rsl.Real_Shop_Name,
    frl.Region_Name,
    ct.Customer_Type_Name,
    'Реализация',
    a.Naimenov,
    fw2.Worker_Name,
    at2.Type_Name,
	CASE
	    WHEN c.ParentCustomer_ID = 0 THEN c.FullName
	    ELSE (
	        SELECT TOP 1 c_parent.FullName
	        FROM shops.dbo.Customer c_parent
	        WHERE c_parent.Customer_ID = c.ParentCustomer_ID
	        ORDER BY c_parent.Customer_ID
	    )
END AS GroupName,
CASE
    WHEN c.ParentCustomer_ID IN (111946, 111936) THEN 'Фед.дистр'
    WHEN c.Customer_Type_ID = 22 THEN 'РАЭК'
    WHEN (c.division_id = 6 OR c.division_id = 4) AND c.ParentCustomer_ID NOT IN (111946, 111936) AND c.Customer_Type_ID != 22 THEN 'Рег.дистр.'
    WHEN c.division_id = 19 AND c.Region_ID != 10013012 THEN 'Дистр.СНГ'
    WHEN c.Customer_Type_ID = 7 AND c.Customer_Group_ID != 6 THEN 'DIY сети'
    WHEN c.Customer_Type_ID IN (8, 10) AND c.Customer_Group_ID != 6 THEN 'Food сети'
    WHEN c.Customer_Group_ID = 6 THEN 'МП'
    WHEN c.Customer_Type_ID = 15 THEN 'OEM'
    ELSE 'прочие'
END AS ClientType
FROM
    IV2012_BT.dbo.[_AccRg1029] f1
    INNER JOIN IV2012_BT.dbo.[_Acc18] F2 ON F2._IDRRef = F1.[_AccountCtRRef]
    INNER JOIN IV2012_BT.dbo.[_Acc18] F3 ON F3._IDRRef = F1.[_AccountDtRRef]
    INNER JOIN IV2012_BT.dbo.[_Document227] d2 ON d2.[_IDRRef] = F1.[_RecorderRRef]
    INNER JOIN DMR_Sklad.dbo.real_naklad rn ON rn.Real_Naklad_ID = d2.[_Fld4881_N]
    INNER JOIN DMR_Sklad.dbo.real_string rs ON rn.Real_Naklad_ID = rs.Real_Naklad_ID
    INNER JOIN DMR_sklad.dbo.assortim a ON rs.Real_String_TovCode = a.TovCode
    INNER JOIN DMR_sklad.dbo.tovgr tovgr ON a.TovGr = tovgr.TovGrCode
    INNER JOIN DMR_Sklad.dbo.tovpgr tovpgr ON a.TovPGr = tovpgr.TovPGrCode
    INNER JOIN DMR_Sklad.dbo.tovspgr spgr ON a.TovSPgr = spgr.TovSPGrCode
    INNER JOIN DMR_Sklad.dbo.tovupgr upgr ON a.TovUPgr = upgr.TovUPGrCode
    INNER JOIN dmr_sklad.dbo.real_store rst ON rs.Real_Store_ID = rst.Real_Store_ID
    INNER JOIN dmr_sklad.dbo.real_shop_list rsl ON rst.Real_Shop_ID = rsl.Real_Shop_ID
    INNER JOIN shops.dbo.CUSTOMER c ON rsl.Real_Shop_ShopCode = c.Customer_ID
    INNER JOIN shops.dbo.CURATOR c2 ON c.Curator_ID = c2.Curator_ID
    INNER JOIN DMR_Sklad.dbo.f_workers fw ON c2.Worker_ID = fw.Worker_ID
    INNER JOIN DMR_Sklad.dbo.f_region_list frl ON c.Region_ID = frl.Region_ID
    INNER JOIN shops.dbo.Customer_Type ct ON c.Customer_Type_ID = ct.Customer_Type_ID
    INNER JOIN DMR_Sklad.dbo.cer_Data_Worker cdw ON a.TovCode = cdw.TovCode
    INNER JOIN DMR_Sklad.dbo.f_workers fw2 ON cdw.Worker_ID = fw2.Worker_ID
    INNER JOIN DMR_Sklad.dbo.assortim_type at2 ON a.Type_ID = at2.Type_ID
WHERE
    (f2.[_Code] = '41.1' OR f2.[_Code] = '41.3') AND f3.[_Code] = '45'
    AND f1.[_Period] BETWEEN '4021.01.01' and '4024.01.01'
GROUP BY
    f1.[_Period],
    f1.[_LineNo],
    d2.[_Number],
    f2.[_Code],
    rs.Real_String_Amount * rs.Real_String_PriceR,
    rs.Real_String_Amount * rs.Real_String_Price,
    rs.Real_String_Amount * rs.Real_String_PriceSS,
    rs.Real_String_Amount,
    tovgr.TovGrName,
    tovpgr.TovPGrName,
    spgr.TovSPGrName,
    upgr.TovUPGrName,
    fw.Worker_Name,
    rsl.Real_Shop_Name,
    frl.Region_Name,
    ct.Customer_Type_Name,
    a.Naimenov,
    fw2.Worker_Name,
    at2.Type_Name,
    c.ParentCustomer_ID,
    c.FullName,
    c.Customer_Type_ID,Customer_ID,
    c.division_id,
    c.Region_ID,
    c.Customer_Group_ID;

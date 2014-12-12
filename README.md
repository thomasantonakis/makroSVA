makroSVA
========

## Algorithm to calculate Stock Valuation Allowance according to Metro Group Policy

### Pre-requisites

All algorithm has to be run in a single folder. This will be the working directory for the current month execution of the Stock Valuation Allowance according to the MetroGroup Policy.

In the working directory, there has to be another folder named `Input`, where the following files have to exist: 

1. SVA.mdb
2. 99_init.xls
3. Stores_10_11.xls
4. Percentage_Use.xls
5. SVA_COP.xls
6. SVA_Sell_costs.xls
7. Customer_Discounts.xls (or optionally a folder named `CU_Discounts` containing 9 files.)
8. Stock_Valuation_COMS.xls (or something containing the Aging Factors per Article Group
9. SO_per_Group.xls
10. vs_Adj (for reconciliation)


### Steps - Check List

1. Read From Access the Stores File
1. Read From Access the Warehouses File
1. Join TP99 with HO prices
1. Read from Xls stores 10 and 11
1. Check Stock Value with Stat_Margin
1. Unify 9 stores with Kalamata and Chania
1. Read COP_expenses
1. Read Selling Cost Expenses
1. Read Percentage Use 20.40
1. Read Percentage Use Retros 
1. Read Percentage Use ICD's
1. Read Aging
1. Read SO_per Group
1. Read Customer Discounts
1. Third Party Allocation (Step 1 of 3) Percentage Calculation
1. Third Party Allocation (Step 2 of 3) TL allocation for each store & article
1. Third Party Allocation (Step 3 of 3) Connect to stores
1. Check with Bperf
1. Clean Up
1. Calculating Step 1 - ICD's
1. Calculating Step 2 - Supplier Discounts
1. Calculating Step 3 - OCOP
1. Calculating Step 4 - Customer Discounts
1. ReUnite the Finals?
1. Calculating Step 5 - Personnel Expenses
1. Calculating Step 6 - Selling Costs
1. Calculating Step 7 - Sellouts
1. Calculating Step 8 - Promo Effect
1. Calculating Step 9 - Aging % and Effect
1. Calculating Step 10 - COP / NRV / Stock Depreciation
1. Clean Up finals
1. Checks with Adj
1. EXPORTS
1. Summaries
1. Check Reproducibility
1. Break Down in simple .R scripts
1. Ask for input
1. Test with Other month Data


### Outputs

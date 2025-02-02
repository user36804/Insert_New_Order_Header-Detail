go
create or alter procedure InsertNewOrder_Header_Detail (
	@DueDate datetime, @CustomerID int, @BillToAddressID int, @ShipToAddressID int, @ShipMethodID int,	--OrderHeader
	@OrderQuantities nvarchar(max), @ProductIDs nvarchar(max), @SpecialOfferID int, @UnitPrice money	--OrderDetail
)	
as
begin
		drop table if exists #tempTable
		drop table if exists #mergingTable
		drop table if exists #tempProductsAvailable
		drop table if exists #tempProductsNotAvailable

-------------------------------------------------TEMPORARY TABLES----------------------------------------------------------------
	create table #tempTable(		--tabel in care introducem ProductIDs si Order Quantities
		ProductID_temp int,
		OrderQuantity_temp int
	)

	create table #mergingTable(		--tabel folosit pentru a face merge intre rezultatul CTE-ului si Production.ProductInventory
		ProductID_merge int,
		LocationID_merge int,
		OrderQuantity_merge int
	)

	create table #tempProductsNotAvailable(	--Produsele care nu sunt in stoc/nu au stoc suficient
		ProductID_notAvailable int
	)

	create table #tempProductsAvailable(		--Produsele care au stoc suficient
		[index] int identity (1,1),
		ProductID_Available int,
		OrderQuantity_Available int
	)

----------------------------------------------PRODUCT IDs, ORDER QTYs-----------------------------------------------------------
	insert into #tempTable( ProductID_temp, OrderQuantity_temp )	--Populam tempTable
		select prod.value, quant.value 
		from string_split(@productIDs, ',', 1) as prod
		join string_split(@OrderQuantities, ',',1) as quant
		on prod.ordinal = quant.ordinal
	
	--start CTE care asociaza orderquantity cu stocul
	;with result1 as	--Caclulam maximul ca suma dintre stocuri
	(
		select productID, LocationID, Quantity, sum(Quantity) over (
			Partition by ProductID
			Order by LocationID asc	
		) as [Sum Stock Quantity]
		from Production.ProductInventory
	),
	result2 as --aloca o cantitate de scazut pentru fiecare loc. id
	(
		select tt.ProductID_temp, r1.LocationID, r1.Quantity, tt.OrderQuantity_temp,
		r1.[Sum Stock Quantity], 
		/* 3 Situatii:
		1. Comanda mai MICA si decat stocul, si mai MICA decat suma -> Scadem din LocationID cu cel mai mare stoc
		2. Comanda mai MARE decat stocul, dar mai MICA decat suma	-> Scadem din prima LocationID tot, din a doua restul
		3. Comanda mai MARE decat stoc si mai MARE decat suma		-> NULL, nu putem distribui cantitatile pentru ca sunt insuficiente */
		case 
			when OrderQuantity_temp <= [Sum Stock Quantity] - Quantity	--1
				then 0
			when OrderQuantity_temp <= [Sum Stock Quantity]	--2
				then OrderQuantity_temp - ( [Sum Stock Quantity] - Quantity)	--scade din orderquantoty diferenta
			when OrderQuantity_temp <= Quantity		--varianta situatia 1
				then OrderQuantity_temp	
			else
				Quantity		--completare la situatia 2
			end
			as Distributii
		from #tempTable as tt	
		join result1 as r1
		on tt.ProductID_temp = r1.productID
	),		
	result3 as
	(
		select ProductID_temp from result2
		group by ProductID_temp
		having sum(Distributii) >= max(OrderQuantity_temp)		--situatia 3, filtram produsele ce nu pot fi cumparate
	)
	insert into #mergingTable ( ProductID_merge, LocationID_merge, OrderQuantity_merge)	--Populam mergingTable
	select r3.ProductID_temp, LocationID, Distributii
	from result3 as r3
	join result2 as r2
	on r2.ProductID_temp = r3.ProductID_temp
	join Production.[Product] as prod
	on prod.ProductID = r3.ProductID_temp
	--end CTE

	select * from #tempTable
	select * from #mergingTable		--uncomment ca sa vezo rezultatul CTE-ului

	insert into #tempProductsNotAvailable(ProductID_notAvailable)	--Populam tempProdNotAvailable
	select m.ProductID_merge 
	from #mergingTable as m
	left join #tempTable as t
		on m.ProductID_merge = t.ProductID_temp
	where t.ProductID_temp is not null
	group by m.ProductID_merge, t.ProductID_temp

	if(exists (	select * from #tempProductsNotAvailable) )		--Daca avem produse care nu au stoc suficient sau deloc, 
	begin
		declare @stringIDs_message nvarchar(255) = 'The following products could not be ordered due to insufficient stock: ' + 
			(
				select STRING_AGG(ProductID_notAvailable, ', ') 
				from #tempProductsNotAvailable
			)
		raiserror(@stringIDs_message, 16, 1);		--...facem un raise error in care mentionam productIDs
	end

	else
		print('test')

	insert into #tempProductsAvailable(ProductID_Available, OrderQuantity_Available)	--Populam tempProdAvailable
	select ProductID_merge, sum(OrderQuantity_merge)
	from #mergingTable as m
	group by ProductID_merge

	select * from #tempProductsAvailable
	select * from #tempProductsNotAvailable

-----------------------------------------------SALES ORDER HEADER---------------------------------------------------------
	begin try
		begin transaction;
			insert into Sales.SalesOrderHeader( DueDate, CustomerID, BillToAddressID, ShipToAddressID, ShipMethodID )
			values ( @DueDate, @CustomerID, @BillToAddressID, @ShipToAddressID, @ShipMethodID )
		commit transaction

		declare @getSalesOrderID int = scope_identity() -- ultima identitate introd. in sesiune
		if(@getSalesOrderID) is null	--Daca am intampinat probleme si nu s-a creeat SalesOrderID in SOH, fa rollback
			raiserror('Begin catch error for E1.', 16, 1) --raiserror cu nivel 10-20 sar direct in catch
	end try

	begin catch
		delete from Sales.SalesOrderHeader
		where SalesOrderID = SCOPE_IDENTITY()
		raiserror('E1: Cannot get SalesOrderID from Sales.SalesOrderHeader! Transaction failed.', 16, 1)
	end catch

---------------------------------------SALES ORDER DETAIL---------------------------------------------------------
	declare @salesOrderDetailCount int = (		--de cate iteratii de adaugat in SalesOrderDetail avem nevoie
			select count(*) 
			from #tempProductsAvailable
		)	
	declare @index_While int = 1
	
		begin try		--Incercam sa introducem in SOD comanda aferenta SOH
		begin transaction;

			while(@index_While <= @salesOrderDetailCount)
			begin
					insert into Sales.SalesOrderDetail(SalesOrderID, SpecialOfferID, UnitPrice, ProductID, OrderQty)
					values(@getSalesOrderID, @SpecialOfferID,
						(
							select ListPrice from #tempProductsAvailable as tpa
							join [Production].[Product] as prod
							on tpa.ProductID_Available = prod.ProductID
							where [index] = @index_While
						),
						(
							select ProductID_Available			--ProductIDs
							from #tempProductsAvailable
							where [index] = @index_While
						),
						
						(
							select sum(OrderQuantity_Available)		--OrderQty's
							from #tempProductsAvailable
							where [index] = @index_While
						)
					)
					set @index_While = @index_While + 1		--incrementam index-ul de parcurgere a buclei

--Pentru vizualizari:
--select ProductID_Available	 as ProductIDs		--ProductIDs
--from #tempProductsAvailable
--where [index] = @@index_While
--
--select sum(OrderQuantity_Available)	as SumQuant	--OrderQty's
--from #tempProductsAvailable
--where [index] = @@index_While

			end

			if( 
				(	select count(*)  
					from Sales.SalesOrderDetail
					where SalesOrderID = @getSalesOrderID	) <> @salesOrderDetailCount
			  )	--daca nu am introdus cate randuri trebuia in SOD

			raiserror('Begin catch error for E2.', 16, 1)

		commit transaction;
		end try

		begin catch
		IF @@TRANCOUNT > 0
			begin
				raiserror('E2: An error occurred while inserting details in SalesOrderDetails! 
				Order details were rolled back in tables "SalesOrderDetail" and "SalesOrderHeader".' ,16,1)

				rollback transaction;
			
				delete from Sales.SalesOrderHeader
				where SalesOrderID = SCOPE_IDENTITY()
			end
		end catch

---------------------------------------ACTUALIZARE STOC PRODUCTION.PRODUCTINVENTORY-----------------------------------------

	begin try
		merge Production.ProductInventory as tgt
		using #mergingTable as src
			on tgt.ProductID = src.ProductID_merge
			and tgt.LocationID = src.LocationID_merge
		when matched
		then update
			set tgt.Quantity = tgt.Quantity - src.OrderQuantity_merge;


		if(
			(
				select max(Quantity) 
				from [Production].[ProductInventory] 
				where ProductID in (
					select ProductID_Available from #tempProductsAvailable
					) 
			) < 0		--veriicam daca am facut stocul negativ la unul din productIDs in tabelul ProductInventory
		)
		raiserror('Begin catch error for E3.', 16, 1)
	end try


	begin catch
		IF @@TRANCOUNT > 0
		begin
			raiserror('E3: Negative deposit stock! Order details were rolled back in tables "SalesOrderDetail" and "SalesOrderHeader".' ,16,1)
			rollback transaction;
		
			delete from Sales.SalesOrderHeader
			where SalesOrderID = SCOPE_IDENTITY()
		end
	end catch
end


----------------------------EXAMPLE USAGE-----------------------------------
--begin tran
--declare @orderDate_Plus7 datetime = dateadd(day, 7, getdate())
--exec InsertNewOrder_Header_Detail 
--@DueDate = @orderDate_Plus7, @CustomerID = 29825, @BillToAddressID = 985, @ShipToAddressID = 985, @ShipMethodID = 5,	--OrderHeader
--@OrderQuantities = '12, 27, 12, 2800', 
--@ProductIDs		 = '963, 964, 966, 967', @SpecialOfferID = 1, @UnitPrice = 17	--OrderDetail
--
--commit
---------------------------------------------------------------



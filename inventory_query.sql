select InventoryKey, ItemMasterKey, DateKey,Quantity,
sum(Quantity) over (partition by ItemMasterKey
					order by DateKey
                    rows between unbounded preceding and current row
                    ) as running_total
from inventory_fact;

select InventoryKey, ItemMasterKey, DateKey,Quantity,
sum(Quantity) over (partition by ItemMasterKey
				    order by DateKey
                    rows between 2 preceding and current row
                    ) as running_total
from inventory_fact;


select InventoryKey, ItemMasterKey, DateKey,Quantity,
sum(Quantity) over (partition by ItemMasterKey
				     order by InventoryKey
                    range between 5 preceding and current row
                    ) as running_total
from inventory_fact;

select InventoryKey, ItemMasterKey, DateKey,Quantity,
sum(Quantity) over (partition by ItemMasterKey
				    order by InventoryKey
                    rows between current row and 3 following
                    ) as running_total
from inventory_fact;


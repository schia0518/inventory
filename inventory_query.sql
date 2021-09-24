select InventoryKey, ItemMasterKey, DateKey,Quantity,
sum(Quantity) over (partition by ItemMasterKey
				     order by DateKey
                    rows between unbouned preceding and current row
                    ) as running_total
from inventory_fact;

select InventoryKey, ItemMasterKey, DateKey,Quantity,
sum(Quantity) over (partition by ItemMasterKey
				    order by DateKey
                    rows between current row and 8 following
                    ) as running_total
from inventory_fact;


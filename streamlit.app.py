# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col


# Write directly to the app
st.title(f"Pending Smoothie Orders :cup_with_straw:")
st.write(
  """
  **Orders that need to be filled!!**.
  """
)

session = get_active_session()
##my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).collect()
##st.dataframe(data=my_dataframe, use_container_width='TRUE')

pending_rows = (
    session
        .table("smoothies.public.orders")
        .select("order_uid","NAME_ON_ORDER", "INGREDIENTS","order_filled")
        .filter(col("ORDER_FILLED") == 0)
        .collect()
)

pending_df = pd.DataFrame([r.as_dict() for r in pending_rows])

if pending_df.empty:
  st.info("No pending orders!!!")
  st.stop()

if "FILL" not in pending_df.columns:
    pending_df.insert(0,"FILL", False)

edited_df = st.data_editor(
    pending_df,
    hide_index=True,
    use_container_width=True,
    column_order=["FILL", "NAME_ON_ORDER", "INGREDIENTS", "ORDER_UID", "ORDER_FILLED"],
    disabled=[ "NAME_ON_ORDER", "INGREDIENTS", "ORDER_UID", "ORDER_FILLED"],
    column_config={
        "FILL": st.column_config.CheckboxColumn("Fill? ✅", default=False),
        "NAME_ON_ORDER": st.column_config.TextColumn("Name on order"),
        "INGREDIENTS": st.column_config.TextColumn("Ingredients"),
        "ORDER_UID": st.column_config.TextColumn("Order UID"),
        "ORDER_FILLED": st.column_config.CheckboxColumn("Already filled", disabled=True),
    },
)


if st.button("Submit", icon=":material/thumb_up:"):
    selected_uids = edited_df.loc[edited_df["FILL"] == True, "ORDER_UID"].tolist()

    if not selected_uids:
        st.warning("No orders selected.")
    else:
        # Atualiza com Snowpark (evita concatenar SQL)
        orders_table = session.table("smoothies.public.orders")
        orders_table.update(
            {"ORDER_FILLED": True},
            col("ORDER_UID").isin(selected_uids)
        )

        st.success(f"Updated {len(selected_uids)} order(s) ✅", icon="✅")
        st.rerun()

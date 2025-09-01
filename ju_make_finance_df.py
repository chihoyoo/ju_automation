import pandas as pd
import streamlit as st


def make_finance_df(
    df_final: pd.DataFrame,
    drive_files: list,
    quantity_column: str | None,
    shipping_fee_sale: int | None,
    seller_shipping_ratio: int | None,
    island_fee_input: int | None,
) -> pd.DataFrame:
    """df_final로부터 정산 요약 df_finance를 생성합니다.

    컬럼 순서: '상품명','옵션','수량','공구판매가','공구판매가합계(vat포함)','공급가(vat포함)','정산금액(vat포함)'
    """
    # 상품명 라벨: 드라이브 첫 파일명 기준 "{3번째요소}X{4번째요소}"
    fname = (drive_files[0].get("name") if (isinstance(drive_files, list) and len(drive_files) > 0 and isinstance(drive_files[0], dict)) else "") or ""
    base_name = fname.rsplit(".", 1)[0]
    parts = base_name.split("_")
    product_label = f"{parts[2]}X{parts[3]}" if len(parts) >= 4 else base_name

    rows: list[dict] = []

    # 옵션별 집계
    if df_final is not None and not df_final.empty:
        grp = df_final.groupby("노션상품", dropna=False)
        for opt, g in grp:
            if quantity_column and quantity_column in g.columns:
                qty_series = pd.to_numeric(g[quantity_column], errors="coerce").fillna(0)
            else:
                qty_series = pd.Series(0, index=g.index)
            qty_sum = qty_series.sum()
            unit_sale_series = pd.to_numeric(g["공구판매가"], errors="coerce").dropna() if "공구판매가" in g.columns else pd.Series(dtype=float)
            unit_cost_series = pd.to_numeric(g["공급가(vat포함)"], errors="coerce").dropna() if "공급가(vat포함)" in g.columns else pd.Series(dtype=float)
            sale_sum = (
                pd.to_numeric(g["공구판매가합계(vat포함)"], errors="coerce").fillna(0).sum()
                if "공구판매가합계(vat포함)" in g.columns else 0
            )
            unit_sale = int(unit_sale_series.iloc[0]) if not unit_sale_series.empty else 0
            unit_cost = int(unit_cost_series.iloc[0]) if not unit_cost_series.empty else 0
            settle_sum = int(round(unit_cost * qty_sum))
            rows.append({
                "상품명": product_label,
                "옵션": "" if opt is None else str(opt),
                "수량": int(qty_sum),
                "공구판매가": unit_sale,
                "공구판매가합계(vat포함)": int(round(sale_sum)),
                "공급가(vat포함)": unit_cost,
                "정산금액(vat포함)": settle_sum,
            })

    # 비율
    seller_ratio = 100 if seller_shipping_ratio is None else max(0, min(100, int(seller_shipping_ratio)))

    # 배송비 row
    ship_fee_sale = int(shipping_fee_sale or 0)
    if df_final is not None and "배송비" in df_final.columns:
        ship_cnt = int((pd.to_numeric(df_final["배송비"], errors="coerce").fillna(0) > 0).sum())
    else:
        ship_cnt = 0
    if ship_cnt > 0 and ship_fee_sale > 0:
        ship_cost_unit = int(round(ship_fee_sale * (seller_ratio / 100)))
        rows.append({
            "상품명": product_label,
            "옵션": "배송비",
            "수량": ship_cnt,
            "공구판매가": int(ship_fee_sale),
            "공구판매가합계(vat포함)": int(ship_cnt * ship_fee_sale),
            "공급가(vat포함)": ship_cost_unit,
            "정산금액(vat포함)": int(ship_cnt * ship_cost_unit),
        })

    # 도서산간배송비 row
    if df_final is not None and "도서산간배송비" in df_final.columns:
        island_cnt = int((pd.to_numeric(df_final["도서산간배송비"], errors="coerce").fillna(0) > 0).sum())
    else:
        island_cnt = 0
    if island_cnt > 0:
        island_fee_sale = int(island_fee_input or 0)
        if island_fee_sale <= 0:
            # df_final의 도서산간배송비 평균(셀러부담 금액) → 원래 판매가로 역산
            avg_fee_series = pd.to_numeric(df_final["도서산간배송비"], errors="coerce").fillna(0) if "도서산간배송비" in df_final.columns else pd.Series(dtype=float)
            avg_cost = float(avg_fee_series[avg_fee_series > 0].mean()) if (avg_fee_series > 0).any() else 0.0
            if avg_cost > 0 and seller_ratio > 0:
                island_fee_sale = int(round(avg_cost / (seller_ratio / 100)))
            else:
                island_fee_sale = int(round(avg_cost))
        if island_fee_sale > 0:
            island_cost_unit = int(round(island_fee_sale * (seller_ratio / 100)))
            rows.append({
                "상품명": product_label,
                "옵션": "도서산간배송비",
                "수량": island_cnt,
                "공구판매가": island_fee_sale,
                "공구판매가합계(vat포함)": int(island_cnt * island_fee_sale),
                "공급가(vat포함)": island_cost_unit,
                "정산금액(vat포함)": int(island_cnt * island_cost_unit),
            })

    df_finance = pd.DataFrame(rows, columns=[
        "상품명","옵션","수량","공구판매가","공구판매가합계(vat포함)","공급가(vat포함)","정산금액(vat포함)"
    ])
    return df_finance
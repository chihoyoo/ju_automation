import pandas as pd
import streamlit as st


def make_final_df(
    df_invoice_raw: pd.DataFrame,
    df_notion: pd.DataFrame,
    df_matching: pd.DataFrame,
    product_column: str,
    option_column: str | None = None,
    quantity_column: str | None = None,
    order_number_column: str | None = None,
    shipping_fee: int | None = None,
    shipping_condition_amount: int | None = None,
    seller_shipping_ratio: int | None = 100,
    island_column: str | None = None,
    island_mode: str | None = None,  # "raw" | "flag"
    island_flag_text: str | None = None,
    island_fee_value: int | None = None,
) -> pd.DataFrame:
    """사용자 선택 컬럼으로 키를 만들어 df_matching과 조인합니다.

    - 매칭 키 형식:
      - 옵션 없음:  {상품명}
      - 옵션 있음:  {상품명}({옵션명})
    - df_matching은 '주문상품' 컬럼을 기준으로 조인됩니다.
    """
    if product_column not in df_invoice_raw.columns:
        raise KeyError(f"상품명 컬럼이 존재하지 않습니다: {product_column}")

    df_result = df_invoice_raw.copy()

    if option_column and option_column != "없음" and option_column in df_result.columns:
        key_series = (
            df_result[product_column].astype(str).str.strip()
            + "("
            + df_result[option_column].astype(str).str.strip()
            + ")"
        )
    else:
        key_series = df_result[product_column].astype(str).str.strip()

    df_result["주문상품"] = key_series.fillna("")

    # 1) 매칭표와 조인하여 df_result에 '노션상품' 열 부여
    merged = df_result.merge(df_matching, on="주문상품", how="left")

    # 2) 노션 테이블에서 '{상품명}({구성})' 키 생성 후 df_result의 '노션상품'과 조인
    if not {"상품명", "구성"}.issubset(df_notion.columns):
        # 필수 컬럼이 없으면 그대로 반환
        return merged

    notion_key_series = (
        df_notion["상품명"].astype(str).str.strip()
        + "("
        + df_notion["구성"].astype(str).str.strip()
        + ")"
    )
    df_notion_with_key = df_notion.copy()
    df_notion_with_key["노션상품키"] = notion_key_series

    final_df = merged.merge(
        df_notion_with_key,
        how="left",
        left_on="노션상품",
        right_on="노션상품키",
        suffixes=("", "_notion"),
    )
    if "노션상품키" in final_df.columns:
        final_df = final_df.drop(columns=["노션상품키"])

    # 3) 금액 계산 컬럼 표준화/생성: (없어도 항상 생성되도록)
    # 수량
    if quantity_column and quantity_column in final_df.columns:
        qty = pd.to_numeric(final_df[quantity_column], errors="coerce").fillna(0)
    else:
        qty = pd.Series(0, index=final_df.index)
    # 단가(판매/공급)
    sale_unit = pd.to_numeric(final_df["공구판매가"], errors="coerce").fillna(0) if "공구판매가" in final_df.columns else pd.Series(0, index=final_df.index)
    cost_unit = pd.to_numeric(final_df["공급가(vat포함)"], errors="coerce").fillna(0) if "공급가(vat포함)" in final_df.columns else pd.Series(0, index=final_df.index)

    # 합계 컬럼 생성(항상 생성)
    final_df["공구판매가합계(vat포함)"] = (sale_unit * qty)
    final_df["공급가합계(vat포함)"] = (cost_unit * qty)
    final_df["정산금액(vat포함)"] = final_df["공급가합계(vat포함)"]

    # 4) 배송비 계산: 주문번호 그룹의 공구판매가 합이 조건 미만이면 첫 행에만 부과
    if (
        order_number_column
        and order_number_column in final_df.columns
        and "공구판매가" in final_df.columns
        and shipping_fee is not None
        and shipping_condition_amount is not None
    ):
        try:
            order_sum = (
                pd.to_numeric(final_df["공구판매가"], errors="coerce").fillna(0)
                .groupby(final_df[order_number_column])
                .transform("sum")
            )
            first_row = ~final_df[order_number_column].duplicated()
            seller_ratio = 100 if seller_shipping_ratio is None else max(0, min(100, int(seller_shipping_ratio)))
            ship_unit = int(round(int(shipping_fee) * seller_ratio / 100))
            final_df["배송비"] = 0
            final_df.loc[first_row & (order_sum < int(shipping_condition_amount)), "배송비"] = ship_unit
        except Exception:
            final_df["배송비"] = 0

    # 5) 도서산간 배송비 처리
    if island_column and island_column in final_df.columns:
        try:
            ratio = 100 if seller_shipping_ratio is None else max(0, min(100, int(seller_shipping_ratio)))
            if island_mode == "raw":
                base_fee = pd.to_numeric(final_df[island_column], errors="coerce").fillna(0)
                final_df["도서산간배송비"] = (base_fee * ratio / 100).round().astype(int)
            elif island_mode == "flag" and order_number_column in final_df.columns:
                flag_mask = final_df[island_column].astype(str).str.contains(str(island_flag_text or ""), na=False)
                is_first = ~final_df[order_number_column].duplicated()
                final_df["도서산간배송비"] = 0
                if island_fee_value is not None:
                    fee_unit = int(round(int(island_fee_value) * ratio / 100))
                    final_df.loc[is_first & flag_mask, "도서산간배송비"] = fee_unit
        except Exception:
            pass

    # 6) 컬럼 정리: 원래 df_raw 컬럼 + 지정 컬럼만 유지
    keep_cols = list(df_invoice_raw.columns) + [
        "노션상품",
        "공구판매가",
        "공급가(vat포함)",
        "공급가합계(vat포함)",
        "공구판매가합계(vat포함)",
        "정산금액(vat포함)",
        "공구판매가",
        "공구판매가합계(vat포함)",
        "배송비",
        "도서산간배송비",
    ]
    existing = [c for c in keep_cols if c in final_df.columns]
    final_df = final_df.loc[:, existing]
    return final_df



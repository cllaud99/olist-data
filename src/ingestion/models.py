from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, constr

StateType = Literal[
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
]


class ConfigBase(BaseModel):
    class Config:
        str_strip_whitespace = True
        json_encoders = {datetime: lambda v: v.isoformat()}
        alias_generator = lambda x: x.replace(" ", "_")


class OlistCustomer(ConfigBase):
    customer_id: str = Field(..., description="Identificador único do cliente")
    customer_unique_id: str = Field(
        ...,
        description="Identificador único por cliente, considerando que um cliente pode ter mais de um ID",
    )
    customer_zip_code_prefix: str = Field(
        ..., description="Prefixo do código postal do cliente"
    )
    customer_city: str = Field(..., description="Cidade do cliente")
    customer_state: StateType = Field(..., description="Estado do cliente")


class OlistGeolocation(ConfigBase):
    geolocation_zip_code_prefix: str = Field(
        ..., description="Prefixo do código postal da geolocalização"
    )
    geolocation_lat: float = Field(..., description="Latitude da geolocalização")
    geolocation_lng: float = Field(..., description="Longitude da geolocalização")
    geolocation_city: str = Field(..., description="Cidade da geolocalização")
    geolocation_state: StateType = Field(..., description="Estado da geolocalização")


class OlistOrderItem(ConfigBase):
    order_id: str = Field(..., description="Identificador único do pedido")
    order_item_id: int = Field(..., description="Identificador único do item do pedido")
    product_id: str = Field(..., description="Identificador do produto")
    seller_id: str = Field(..., description="Identificador do vendedor")
    shipping_limit_date: datetime = Field(..., description="Data limite para o envio")
    price: float = Field(..., description="Preço do item")
    freight_value: float = Field(..., description="Valor do frete")


class OlistOrderPayment(ConfigBase):
    order_id: str = Field(..., description="Identificador único do pedido")
    payment_sequential: int = Field(..., description="Número sequencial do pagamento")
    payment_type: Literal[
        "credit_card", "boleto", "voucher", "debit_card", "not_defined"
    ] = Field(..., description="Tipo de pagamento")
    payment_installments: int = Field(
        ..., description="Número de parcelas do pagamento"
    )
    payment_value: float = Field(..., description="Valor do pagamento")


class OlistOrderReview(ConfigBase):
    review_id: str = Field(..., description="Identificador único da avaliação")
    order_id: str = Field(..., description="Identificador único do pedido")
    review_score: int = Field(..., description="Pontuação da avaliação (1 a 5)")
    review_comment_title: Optional[constr(max_length=255)] = Field(None, description="Título do comentário da avaliação")  # type: ignore
    review_comment_message: Optional[constr(max_length=1000)] = Field(None, description="Mensagem do comentário da avaliação")  # type: ignore
    review_creation_date: datetime = Field(
        ..., description="Data de criação da avaliação"
    )
    review_answer_timestamp: Optional[datetime] = Field(
        None, description="Data e hora da resposta da avaliação"
    )


class OlistOrder(ConfigBase):
    order_id: str = Field(..., description="Identificador único do pedido")
    customer_id: str = Field(..., description="Identificador único do cliente")
    order_status: str = Field(..., description="Status do pedido")
    order_purchase_timestamp: datetime = Field(
        ..., description="Data e hora da compra do pedido"
    )
    order_approved_at: Optional[datetime] = Field(
        None, description="Data e hora da aprovação do pedido"
    )
    order_delivered_carrier_date: Optional[datetime] = Field(
        None, description="Data em que o pedido foi entregue ao transportador"
    )
    order_delivered_customer_date: Optional[datetime] = Field(
        None, description="Data em que o pedido foi entregue ao cliente"
    )
    order_estimated_delivery_date: Optional[datetime] = Field(
        None, description="Data estimada de entrega do pedido"
    )


class OlistProduct(ConfigBase):
    product_id: str = Field(..., description="Identificador único do produto")
    product_category_name: str = Field(..., description="Categoria do produto")
    product_name_length: int = Field(
        ...,
        alias="product_name_length",
        description="Comprimento do nome do produto em caracteres",
    )
    product_description_length: int = Field(
        ...,
        alias="product_description_length",
        description="Comprimento da descrição do produto em caracteres",
    )
    product_photos_qty: int = Field(..., description="Quantidade de fotos do produto")
    product_weight_g: int = Field(..., description="Peso do produto em gramas")
    product_length_cm: int = Field(
        ..., description="Comprimento do produto em centímetros"
    )
    product_height_cm: int = Field(..., description="Altura do produto em centímetros")
    product_width_cm: int = Field(..., description="Largura do produto em centímetros")


class OlistSeller(ConfigBase):
    seller_id: str = Field(..., description="Identificador único do vendedor")
    seller_zip_code_prefix: str = Field(..., description="Prefixo do CEP do vendedor")
    seller_city: str = Field(..., description="Cidade do vendedor")
    seller_state: StateType = Field(..., description="Estado do vendedor")

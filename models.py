from pydantic import BaseModel, field_validator

# Modelo para los datos que se recibirán
class MovfPago(BaseModel):
    age_id: int
    fpago_id: int
    movfp_desde: int
    movfp_hasta: int
    cbu: str
    vencimiento: str
    movfp_id : int

    # Validador para el campo cbu
    @field_validator("fpago_id")
    def validate_fpago(cls, value):
        if not value in (1,2,3):
            raise ValueError("La forma de pago ingresada NO es valida.")
        return value
    
    # Validador para el campo movfp_desde
    @field_validator("movfp_desde")
    def validate_movfp_desde(cls, value):
        if not (100000 <= value <= 999999):
            raise ValueError("movfp_desde debe tener 6 dígitos en formato yyyymm")
        year = value // 100
        month = value % 100
        if not (1 <= month <= 12):
            raise ValueError("movfp_desde debe tener un mes válido entre 01 y 12")
        return value

    # Validador para el campo movfp_hasta
    @field_validator("movfp_hasta")
    def validate_movfp_hasta(cls, value):
        if not (100000 <= value <= 999999):
            raise ValueError("movfp_hasta debe tener 6 dígitos en formato yyyymm")
        year = value // 100
        month = value % 100
        if not (1 <= month <= 12):
            raise ValueError("movfp_hasta debe tener un mes válido entre 01 y 12")
        return value
    
    # Validador para el campo cbu
    @field_validator("cbu")
    def validate_cbu(cls, value):
        cbu_str = value

        # Verificar que el CBU tenga 22 dígitos
        if not cbu_str.isdigit() or len(cbu_str) != 22:
            raise ValueError("El CBU debe contener exactamente 22 dígitos")

        # Validar el primer dígito verificador (posición 8)
        pesos1 = [7, 1, 3, 9, 7, 1, 3]
        suma1 = sum(int(cbu_str[i]) * pesos1[i] for i in range(7))
        verificador1 = (10 - (suma1 % 10)) % 10
        if verificador1 != int(cbu_str[7]):
            raise ValueError("El primer dígito verificador del CBU es incorrecto")

        # Validar el segundo dígito verificador (posición 22)
        pesos2 = [3, 9, 7, 1, 3, 9, 7, 1, 3, 9, 7, 1, 3]
        suma2 = sum(int(cbu_str[i + 8]) * pesos2[i] for i in range(13))
        verificador2 = (10 - (suma2 % 10)) % 10
        if verificador2 != int(cbu_str[21]):
            raise ValueError("El segundo dígito verificador del CBU es incorrecto")

        return value
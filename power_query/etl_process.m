let
    // =========================
    // DATA INGESTION
    // =========================
    Source = Csv.Document(
        File.Contents("source_file.csv"),
        [Delimiter=",", Columns=63, Encoding=65001, QuoteStyle=QuoteStyle.None]
    ),

    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),

    // =========================
    // DATA TYPE STANDARDIZATION
    // =========================
    ChangedType = Table.TransformColumnTypes(PromotedHeaders,{
        {"Pedido", type text},
        {"Município", type text},
        {"UF", type text},
        {"Prazo Cliente", type date},
        {"Transportadora", type text},
        {"Status", type text},
        {"Status (hora processada)", type datetime},
        {"Motorista", type text},
        {"Expedidor", type text},
        {"Data aprovação", type datetime},
        {"Existem Ocorrências", type text},
        {"Última Ocorrência (Status)", type text},
        {"Última Ocorrência (Mensagem)", type text},
        {"Quantidade de Ocorrências", Int64.Type}
    }),

    // =========================
    // REMOVE IRRELEVANT COLUMNS
    // =========================
    RemovedColumns = Table.RemoveColumns(ChangedType,{
        "ID","Rota","Nome","Tipo de Pessoa","CPF/CNPJ","CEP","Bairro","Endereço","Número",
        "Complemento","Itens","Peso (Kg)","Avaliação","Nota 1 CFOP","Nota 1 Chave",
        "Nota 1 Data","Nota 1 Número","Nota 1 Série","Nota 1 Volumes","Valor Total",
        "Prazo","Valor do frete","Status (hora efetuada)","Status (detalhe)","ID Equilibrium",
        "Observações","Data de criação","Placa","Telefone Motorista","Janela Início","Janela Fim",
        "ID Transportadora","Rastreio","Canal de Vendas","Mercadorias","CNPJ","Data de contratação",
        "Tempo de entrega em minutos","Distância em metros","Preço do frete","Valor Nota Fiscal",
        "Tempo médio no cliente","Tempo de Entrega em dias","Pedido de Compra","Data de Coleta",
        "Tags","Código Loja","Loja","Pedido ERP"
    }),

    // =========================
    // DATE STANDARDIZATION
    // =========================
    ConvertUpdateDate = Table.TransformColumnTypes(RemovedColumns,{
        {"Status (hora processada)", type date}
    }),

    RenameUpdateDate = Table.RenameColumns(ConvertUpdateDate,{
        {"Status (hora processada)", "dt_ultima_atualizacao"}
    }),

    ConvertApprovalDate = Table.TransformColumnTypes(RenameUpdateDate,{
        {"Data aprovação", type date}
    }),

    RenameApprovalDate = Table.RenameColumns(ConvertApprovalDate,{
        {"Data aprovação", "dt_aprovacao"}
    }),

    // =========================
    // DATA ENRICHMENT (JOIN)
    // =========================
    MergeStores = Table.NestedJoin(
        RenameApprovalDate,
        {"Expedidor"},
        #"Referência Praças",
        {"Código"},
        "Dim_Lojas",
        JoinKind.LeftOuter
    ),

    ExpandStores = Table.ExpandTableColumn(
        MergeStores,
        "Dim_Lojas",
        {"Loja","Praça"},
        {"Loja","Praça"}
    ),

    // =========================
    // CALCULATED FIELDS
    // =========================
    AddLeadTime = Table.AddColumn(
        ExpandStores,
        "Lead time operacional",
        each Duration.Days(Date.From(DateTime.LocalNow()) - [dt_aprovacao])
    ),

    AddOrderModel = Table.AddColumn(
        AddLeadTime,
        "modelo_pedido",
        each if Text.Contains(Text.Trim([Pedido]), "-1")
            then "Omnichannel"
            else "Delivery"
    ),

    // =========================
    // STATUS CLASSIFICATION
    // =========================
    AddStatusClass = Table.AddColumn(
        AddOrderModel,
        "classificacao_status",
        each
            let status = Text.Trim([Status])
            in
                if List.Contains({"Entregue","Retirado","Devolvido"}, status) then "Finalizado"
                else if List.Contains({
                    "Destinatario ausente","Destinatario mudou-se","Destinatario desconhecido",
                    "Estabelecimento fechado","Dificil acesso","Endereco nao localizado",
                    "Endereco insuficiente","Pacote avariado","Pacote extraviado",
                    "Nao visitado","Carga recusada pelo destinatario","Extravio confirmado"
                }, status) then "Insucesso"
                else if List.Contains({
                    "Aguardando motorista","Em transito","Em rota de coleta",
                    "Saiu para entrega","Coletado"
                }, status) then "Em Rota"
                else "Pendente"
    ),

    // =========================
    // SLA LOGIC
    // =========================
    AddOmniStatus = Table.AddColumn(
        AddStatusClass,
        "Status omnichannel",
        each if [modelo_pedido] = "Omnichannel" then
            if Date.From(DateTime.LocalNow()) <= Date.From([Prazo Cliente])
                then "No prazo"
                else "Fora do prazo"
        else null
    ),

    AddDeliveryStatus = Table.AddColumn(
        AddOmniStatus,
        "Status delivery",
        each if [modelo_pedido] = "Delivery" then
            if [Lead time operacional] <= 3 then "No prazo"
            else if [Lead time operacional] <= 5 then "Atenção"
            else "Crítico"
        else null
    ),

    // =========================
    // FINAL MONITORING STATUS
    // =========================
    AddMonitoringStatus = Table.AddColumn(
        AddDeliveryStatus,
        "Status monitoramento",
        each if [modelo_pedido] = "Delivery" then [Status delivery]
        else if [modelo_pedido] = "Omnichannel" then [Status omnichannel]
        else null
    )

in
    AddMonitoringStatus

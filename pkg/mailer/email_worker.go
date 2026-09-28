package mailer

import (
	"context"
	"log"

	"github.com/matheusbanqueiro/gocommerce-tools/pkg/channels"
	"github.com/matheusbanqueiro/gocommerce-tools/pkg/events"
)

// StartEmailWorker inicia um Worker Pool de Goroutines para processar a fila de e-mails em background.
func StartEmailWorker(ctx context.Context, workersCount int) {

	// Se o número de workers for menor ou igual a zero, define o valor padrão como 3.
	if workersCount <= 0 {
		workersCount = 3
	}

	// Inicia os workers para processar eventos de e-mail de redefinição de senha.
	for w := 1; w <= workersCount; w++ {
		go func(workerID int) {
			log.Printf("[Worker %d - gocommerce-utils] Operário de e-mails iniciado via Goroutine...\n", workerID)
			for {
				select {
				case <-ctx.Done():
					log.Printf("[Worker %d - gocommerce-utils] Operário finalizado.\n", workerID)
					return
				case evt, ok := <-channels.PasswordResetChan:
					if !ok {
						return
					}
					processPasswordResetEmail(workerID, evt)
				}
			}
		}(w)
	}
}

func processPasswordResetEmail(workerID int, evt events.PasswordResetEvent) {
	log.Printf("\n======================================================================\n")
	log.Printf("[Worker #%d | gocommerce-utils] 📧 SIMULAÇÃO DE ENVIO DE E-MAIL\n", workerID)
	log.Printf("Para: %s <%s>\n", evt.UserName, evt.Email)
	log.Printf("Assunto: Sua Nova Senha de Acesso ao GoCommerce\n")
	log.Printf("Mensagem: Olá %s, sua nova senha gerada é: [%s]\n", evt.UserName, evt.TemporaryPassword)
	log.Printf("Utilize esta senha para realizar o seu login na plataforma.\n")
	log.Printf("======================================================================\n\n")
}
